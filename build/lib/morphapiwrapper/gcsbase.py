from google.cloud import storage
import random
import string
import os

storageclient = storage.Client()

remotedataheaders={'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
	   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
	   'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
	   'Accept-Encoding': 'none',
	   'Accept-Language': 'en-US,en;q=0.8',
	   'Connection': 'keep-alive'}

class google_storage_helper:
	def __init__(self,userid,username,runid,apiname = 'dummy',workingbucket = "morphisdomtempworkspace",localdir="./"):
		self.workingbucket = workingbucket
		self.localdir = localdir
		self.apiname = apiname
		self.username = username
		self.userid = userid
		self.runid = runid
	
	def upload_to_gcs(self,path_to_localfile):
		workingbucket_handle = storageclient.get_bucket(self.workingbucket)
		filename = path_to_localfile.split("/")[-1]+self.apiname
		if not filename.startswith(self.runid ):
			filename = self.runid  + filename
		blob = workingbucket_handle.blob(self.userid+'/'+filename)
		data = blob.upload_from_filename(path_to_localfile)
		output= 'gs://'+self.workingbucket+'/'+self.userid+'/'+filename
		return output
		
	def processoutput(self,outputdata):
		localfilename = str(self.localdir+''.join(random.choices(string.ascii_uppercase + string.digits, k = 8)))
		if  isinstance(outputdata, list):
			output =  [ self.processoutput(i) for i in outputdata ] 
			return output
		elif isinstance(outputdata, dict):
			output = {}
			for k,v in outputdata.items():
				output[k] = self.processoutput(v) 
			return output
		elif isinstance(outputdata, bytes):
			with open(localfilename, 'wb') as newfile:
				newfile.write(outputdata)
			return self.upload_to_gcs(localfilename)

		elif isinstance(outputdata, str) and len(outputdata) < 200:
			if os.path.isfile(outputdata):
				return self.upload_to_gcs(outputdata)
			else:
				return outputdata
		elif len(str(outputdata)) > 10**6:
			with open(localfilename, 'w') as newfile:
				newfile.write(str(outputdata))
			return self.upload_to_gcs(localfilename)
		return outputdata
		
	def download_input_data(self,inputdata,returndata = False):
		localfilename = str(self.localdir+''.join(random.choices(string.ascii_uppercase + string.digits, k = 8)))
		#print("inputdata type", type(inputdata))
		########### check input type #######
		if isinstance(inputdata, bytes):
			try:
				data = inputdata.decode('utf-8')
			except:
				data = str(inputdata)
			if not returndata:
				with open(localfilename, 'wb') as newfile:
					newfile.write(inputdata)
				return localfilename
			else:
				return data
		elif isinstance(inputdata, str):
			######## is it gcs? ####
			if inputdata.startswith("gs://"):
				sourcebucketname = inputdata.split("gs://")[1].split("/")[0]
				sourceblobname = inputdata.split("gs://")[1].split(sourcebucketname+"/")[1]
				sourcefilename = inputdata.split("/")[-1].split(".")[0]
				sourcebucket = storageclient.get_bucket(sourcebucketname)
				sourceblob = sourcebucket.blob(sourceblobname)
				localfilename = str(self.localdir+sourcefilename)
				sourceblob.download_to_filename(localfilename)
				if returndata:
					with open(localfilename, 'rb') as file:
						data = file.read()
					os.remove(localfilename)
					return data
				else:
					return localfilename
			elif inputdata.startswith("http://") or inputdata.startswith("https://"):
			####### download file
				httprequest=urllib.request.Request(inputdata,None,remotedataheaders) #The assembled request
				response = urllib.request.urlopen(httprequest)
				data = response.read()
				try:
					data = data.decode('utf-8')
				except:
					data = str(data)
				
				if not returndata:
					with open(localfilename, 'wb') as newfile:
						newfile.write(inputdata)
					return localfilename
				else:
					return data
			else:
				return inputdata
		elif isinstance(inputdata, list):
			output =  [ self.download_input_data(i) for i in inputdata ] 
			return output
		elif isinstance(inputdata, dict):
			output = {}
			for k,v in inputdata.items():
				output[k] = self.download_input_data(v) 
			return output
		else:
			return inputdata
			#raise NameError("Invalid input data")