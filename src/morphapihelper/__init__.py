from flask import *
from morphapiwrapper.gcsbase import *
import requests
import ast
import google.auth.transport.requests
import google.oauth2.id_token
from datetime import datetime,timezone
app = Flask(__name__)


billingurl = "https://us-central1-morphisdombackend.cloudfunctions.net/morphbilling"
@app.route("/",methods=['POST'])
def index():
	return '0',400
	
def appwrapper(func,downloadinput = True,inputtype='data',readwritebase='gcs'):
	def entrypoint():
		headers = request.headers
		if inputtype == 'data':
			input = request.get_data()
			username = headers['sessionuser']
			userid = headers['userid']
			runid = headers['runid']
			try:
				input = input.decode('utf-8')
			except:
				pass
			try:
				input = ast.literal_eval(input)
			except:
				pass
		elif inputtype == 'json':
			input = request.get_json()
		else:
			raise NameError("Invalid inputtype supplied. inputtype should be either 'data' or 'json'")
		apistarttime = datetime.now(timezone.utc)
		#print(input,type(input))
		filereaderwriter = None
		if readwritebase =='gcs' and downloadinput:
			gcsreaderwriter = google_storage_helper(userid,username,runid,apiname = func.__name__)
			#input = gcsreaderwriter.download_input_data(input)
			filereaderwriter = gcsreaderwriter
		elif downloadinput:
			raise NameError("Invalid readwritebase supplied. readwritebase should be 'gcs'")
		
		output,billunit = func(input,filereaderwriter)
		apiendtime=datetime.now(timezone.utc)
		output = gcsreaderwriter.processoutput(output)
		
		if billunit >= 1:
			auth_req = google.auth.transport.requests.Request()
			id_token = google.oauth2.id_token.fetch_id_token(auth_req, billingurl)
			headers = {'Authorization' : 'Bearer '+id_token}
			logdata = {'operation':'apicall','runid':runid,'user':username,'apiname':func.__name__,'quantity':billunit,'runid':runid,'calltime':apistarttime.strftime('%Y-%m-%d %H:%M:%S'),'endtime':apiendtime.strftime('%Y-%m-%d %H:%M:%S'),'runstatus':"complete"}
			x = requests.post(billingurl, json = logdata,headers=headers)
		if isinstance(output,dict):
			return output
		else:
			return str(output)
	return entrypoint
		
def addapproute(app,func,downloadinput=True):
	app.add_url_rule(rule=f"/{func.__name__}",
			endpoint=func.__name__,
			view_func=appwrapper(func,downloadinput),
			methods=["POST"])
	#print(func.__name__,func)
	return app
