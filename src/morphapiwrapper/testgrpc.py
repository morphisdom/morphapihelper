from grpcserve import *
import os
import azure.cognitiveservices.speech as speechsdk
import queue
speech_key="853d745aa313452180a16a2c0764c82f"
service_region="eastus"

class speech_recognition:
	def  __init__(self):
		speech_config = speechsdk.SpeechConfig(subscription=speech_key, region=service_region)
		self.outspeechqueue = queue.Queue()
	# setup the audio stream
		stream = speechsdk.audio.PushAudioInputStream()
		audio_config = speechsdk.audio.AudioConfig(stream=stream)
	# instantiate the speech recognizer with push stream input
		speech_recognizer = speechsdk.SpeechRecognizer(speech_config=speech_config, audio_config=audio_config)
		speech_recognizer.recognized.connect(lambda evt: self.outspeechqueue.put(evt.result.text))
		speech_recognizer.session_started.connect(lambda evt: print('SESSION STARTED: {}'.format(evt)))
		speech_recognizer.session_stopped.connect(lambda evt: print('SESSION STOPPED: {}'.format(evt)))
		speech_recognizer.canceled.connect(lambda evt: print('CANCELED {}'.format(evt)))
		#speech_recognizer.start_continuous_recognition()
		self.stream = stream
		self.speech_recognizer = speech_recognizer
	
	def initialize(self,sessionid):
		self.speech_recognizer.start_continuous_recognition()
	
	def evaluate(self,data,filereaderwriter):
		#print(data)
		self.stream.write(data)
		return get_response()
		
	def get_response(self):
		data = []
		while True:
			try:
				chunk = self.outspeechqueue.get(block=False)
				if chunk is None:
					return
				#print("chunk",chunk)
				data.append(chunk)
				#print("data", data)
			except queue.Empty:
				break
		#print ("returned")
		#print(data)
		return ' '.join(data).encode('utf-8')
	def get_state(self):
		return self
	
	def stop(self):
		self.speech_recognizer.stop_continuous_recognition()

grpcservice = UnaryService(speech_recognition(),stateful= True)
	
#serve(grpcservice, 50051)
