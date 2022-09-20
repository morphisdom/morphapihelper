import grpc
from concurrent import futures
import time
import unary.unary_pb2_grpc as pb2_grpc
import unary.unary_pb2 as pb2
from gcsbase import *
import os
from firebase_admin import initialize_app
import firebase_admin.db as firebasedb
import string  
import random
import ast
client_session = {}
import base64


firebaseapp = initialize_app(options={"projectId": "morphisdom-2890",'databaseURL':"https://morphisdom-runner.firebaseio.com/"})
runnerkeyref = firebasedb.reference('grpcsessionkeys')

#import pyaudio

#audio = pyaudio.PyAudio()
#stream = audio.open(format=pyaudio.paInt16,
 #                   channels=1,
  #                  rate=16000,
   #                 output=True,
    #                frames_per_buffer=3*16000)

class UnaryService(pb2_grpc.UnaryServicer):

	def __init__(self, function, stateful = False, readwritebase ='gcs',maxbufferlen = 1 ):
		self.maxbufferlen = 1
		self.function = function
		self.stateful = stateful
		self.readwritebase = readwritebase
		self.maxbufferlen = maxbufferlen
		
	def set_state(self,sessionid,orderingkey,buffer = []):
		runnersessionref = runnerkeyref.child(sessionid)
		state = self.function.get_state()
		if istinstance(state,str):
			runnersessionref.set({'state':state,'orderingkey':orderingkey,'buffer':buffer})
		else:
			runnersessionref.set({'state':base64.b64encode(bytes(state,'utf-8')).decode('utf-8'),'orderingkey':orderingkey,'buffer':buffer})
	
	def Initiateconn(self,request, context):
	##### Initialize session Connection ###########
		sessionid =  ''.join(random.choices(string.ascii_uppercase + string.digits, k = 16))
		if self.stateful:
			self.function.initialize(sessionid)
			self.set_state(self,sessionid,orderingkey=0)
		metadict = dict(context.invocation_metadata())
		userid = metadict['userid']
		username = metadict['username']
		runid = metadict['runid']
		if self.readwritebase =='gcs':
			self.filereaderwriter = google_storage_helper(userid,username,runid,apiname = self.function.__name__)
		
		result = {'message': sessionid, 'received': True}	
		return pb2.initMessageResponse(**result)

	def GetServerResponse(self, request, context):
		try:
			message = ast.literal_eval(request.message.decode('utf-8')) ### should have (ordering key , bytes)
		except:
			message = request.message.decode('utf-8')
		metadict = dict(context.invocation_metadata())
		if 'stop' in metadict: ### stop current session
			print ("stopping")
			runnersessionref.delete()
			result = {'message': b'stoppped', 'received': True}
			if self.stateful:
				self.function.stop()
			return pb2.MessageResponse(**result)
			
		sessionid = metadict['sessionid']
		if self.stateful:
			try:
				runnersessionref = runnerkeyref.child(sessionid)
				client_session = runnersessionref.get()
			except:
				context.abort(grpc.StatusCode.UNAUTHENTICATED, 'Invalid sessionid')
			orderingkey = message[0]#metadict['orderingkey']
			state = client_session['state']
			buffer = client_session['buffer']
			self.function.set_state(state)
		#buffer = client_session[sessionid][1]
			maxlastbufferorderingkey = client_session['orderingkey']
			buffer.append(message)
		#print (message)
			if len(buffer) > self.maxbufferlen:  ### ignore message if ordering key belongs to last buffer 
		#	buffer.append(message)
				buffer.sort(key = lambda x: x[0])
				maxlastbufferorderingkey = max([maxlastbufferorderingkey,max([i for i,v in buffer])])
				buffer = [v for i,v in buffer]
				result = self.function.evaluate(state,buffer,self.filereaderwriter)
				self.set_state(self,sessionid,orderingkey=maxlastbufferorderingkey)
			else:
				self.set_state(self,sessionid,orderingkey=max([maxlastbufferorderingkey,orderingkey]),buffer=buffer)
		else:
			result = self.function.evaluate(message[1],self.filereaderwriter)
		# if len(buffer)>= self.maxbufferlen:  ### if buffer fills up flush it to speechreco and set maxlastbufferorderingkey and max orderingkey of current buffer
			# buffer.sort(key = lambda x: x[0])
			# maxlastbufferorderingkey = max([i for i,v in buffer])
			# buffer = [v for i,v in buffer]
			#speechclient.write_audio(b''.join(buffer))
			#stream.write(b''.join(buffer))
#		client_session[sessionid][1] = buffer
#		client_session[sessionid][2] = maxlastbufferorderingkey
		################ get text response from speech reco if any
		#result = speechclient.get_response()
		#print (result)
		result = {'message': result, 'received': True}																								
		return pb2.MessageResponse(**result)

def serve(grpcservice, port):
	bind_address = '[::]:'+str(port)
	server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
	pb2_grpc.add_UnaryServicer_to_server(grpcservice, server)
	server.add_insecure_port(bind_address)
	server.start()
	server.wait_for_termination()


#if __name__ == '__main__':
#	serve(50051)