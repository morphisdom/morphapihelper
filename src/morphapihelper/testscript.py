from morphapihelper.__init__ import app, addapproute
import os


def testfunction(input,filereaderwriter): 
	downloadeddata = filereaderwriter.download_input_data(input)
	print(downloadeddata)
	billunit = 2
	return downloadeddata,billunit
	

app = addapproute(app, testfunction, downloadinput = True)
app.run()
