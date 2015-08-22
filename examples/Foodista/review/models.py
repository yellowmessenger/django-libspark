from django.db import models
from mongokit import *
import datetime
import string
import json

connection = Connection()

@connection.register
class Review(Document):
	__collection__ = 'ReviewColl'
	__database__ = 'foodRevPy-dev'
	structure ={
		'message': unicode,
		'fbId' : unicode
	}
	indexes = [
		{
			'fields':['fbId'],
			'unique':True,
		}
	]

@connection.register
class Pagination(Document):
	__collection__ = 'PaginationColl'
	__database__ = 'foodRevPy-dev'
	structure ={
		'next': unicode,
		'create_time': datetime.datetime,
	}
