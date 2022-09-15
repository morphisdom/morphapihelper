# morphapiwrapper
### Wrapper functions for creating APIs in Morphisdom

[![Build Status](https://travis-ci.org/joemccann/dillinger.svg?branch=master)](https://travis-ci.org/joemccann/dillinger)


The morphapiwrapper package contains the wrapper functions to build a API for morphisdom

### Get started
You can install the package by running the following command

```python
pip install morphapiwrapper
```

Then load the package in Python by running the following command
```python
from morphapiwrapper import app, addapproute
```

### Write your custom function
You can write your custom function with this package and serve it as an API in morphisdom
Your custom function should always take two arguments. Namely _requestdata_ and _filereaderwriter_.  
_requestdata_ is the variable which would get the data passed during the API call for the client. 
For multiargument API in morphisdom _requestdata_ holds a list whose elements corresponds to individual arguments of the API function in sequence.
However _requestdata_ can also hold string, number, boolean (for single argument API) and dictionary.
_filereaderwriter_ is an handle for downloading GCS or remote HTTP content into localdirectory. 
if _requestdata_ contains and GCS file URI or HTTP url then the funtion _filereaderwriter.download_input_data(requestdata)_ downloads all those files and returns the name of the local files in the same order. The number of elements in _requestdata_ and _downloadeddata_ remains same.


The following function demonstrates a simple function to be served as a Morphisdom API.
The _requestdata_ is downloaded and returned as output.

In order to log billing API calls the billunit needs to be set greater than 0. 

```python
def testfunction(requestdata,filereaderwriter):
	downloadeddata = filereaderwriter.download_input_data(requestdata)
	print(downloadeddata)
	billunit = 2
	return downloadeddata,billunit
```

### Wrap your function
Once your function is created wrap your function in an flask app with the following code snippet

```python
app = addapproute(app, testfunction)
```

The returned app is a flask app which has a route _/testfunction_ added in it. 
On calling this route _testfunction_ is executed

