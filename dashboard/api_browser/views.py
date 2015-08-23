from django.shortcuts import render
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
import json
from libspark.SparkRDD import SparkRDD

def index(request):
	return render(request, 'api_browser/index.html',{})

@csrf_exempt
def evaluate(request):
	if request.method == "POST":
		print request.body
		decodedJson = json.loads(str(request.body))
		i = 0
		queryString = ''
		exec('data = '+decodedJson[-1])
		dataRDD = SparkRDD(data=data)
		for method in decodedJson[0]:
			if i > 0:
				queryString = queryString + '.'
			i = i+1
			if (method['func'] is True):
				queryString = queryString + method['name'] + '(' + method['lambda'] + ')'
			else:
				queryString = queryString + method['name'] + '()'
		queryString = 'dataRDD.' + queryString + '.' + decodedJson[1]['name'] + '()'
		print queryString
		exec('output = '+queryString)
		print output
	return JsonResponse({'success':output})
