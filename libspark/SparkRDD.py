# Configure the necessary Spark environment
import os
import sys

# Spark home
spark_home = os.environ.get("SPARK_HOME")

# If Spark V1.4.x is detected, then add ' pyspark-shell' to
# the end of the 'PYSPARK_SUBMIT_ARGS' environment variable
spark_release_file = spark_home + "/RELEASE"
if os.path.exists(spark_release_file) and "Spark 1.4" in open(spark_release_file).read():
    pyspark_submit_args = os.environ.get("PYSPARK_SUBMIT_ARGS", "")
    if not "pyspark-shell" in pyspark_submit_args: pyspark_submit_args += " pyspark-shell"
    os.environ["PYSPARK_SUBMIT_ARGS"] = pyspark_submit_args

# Add the spark python sub-directory to the path
sys.path.insert(0, spark_home + "/python")

# Add the py4j to the path.
# You may need to change the version number to match your install
sys.path.insert(0, os.path.join(spark_home, "python/lib/py4j-0.8.2.1-src.zip"))
# Initialize PySpark to predefine the SparkContext variable 'sc'
execfile(os.path.join(spark_home, "python/pyspark/shell.py"))

class SparkRDD():

	# When the SparkRDD is initialized - data and function
	# can be passed as arguments
	def __init__(self, data=[], **kwargs):
		self.func = kwargs.get("func",None)
		self.data = data
		self.is_cached = False
		try:
			self.dataRDD = sc.parallelize (data)
		except TypeError:
			self.dataRDD = data

	def map(self,func):
		mappedData = self.dataRDD.map(func)
		tempObj = SparkRDD(data = mappedData)
		return tempObj

	def filter(self,func):
		filteredData = self.dataRDD.filter(func)
		tempObj = SparkRDD(data = filteredData)
		return tempObj

	def collect(self):
		return self.dataRDD.collect()

	def cache(self):
		self.dataRDD.cache()
		self.is_cached = True
		return self

	def count(self):
		return self.dataRDD.count()

	def distinct(self):
		distinctRDD = self.dataRDD.distinct()
		distinctObj = SparkRDD(data=distinctRDD)
		return distinctObj

	def first(self):
		return self.dataRDD.first()

	def flatMap(self,func):
		flatMapRDD = self.dataRDD.flatMap(func)
		tempObj = SparkRDD(data=flatMapRDD)
		return tempObj

	def flatMapValues(self,func):
		flatMapRDD = self.dataRDD.flatMapValues(func)
		tempObj = SparkRDD(data=flatMapRDD)
		return tempObj

	def groupBy(self,func):
		groupByRDD = self.dataRDD.groupBy(func)
		tempObj = SparkRDD(data=groupByRDD)
		return tempObj

	def groupByKey(self):
		groupByRDD = self.dataRDD.groupByKey()
		tempObj = SparkRDD(data=groupByRDD)
		return tempObj

	def intersection(self,toBeIntersected):
		if isinstance(toBeIntersected,SparkRDD):
			intersected = self.dataRDD.intersection(toBeIntersected.dataRDD)
			tempObj = SparkRDD(data=intersected)
		else:
			#TODO: rasie err
			pass
		return tempObj

	def join(self,toBeJoined):
		if isinstance(toBeJoined,SparkRDD):
			joined = self.dataRDD.join(toBeJoined.dataRDD)
			tempObj = SparkRDD(data=joined)
		else:
			#TODO: rasie err
			pass
		return tempObj

	def keyBy(self,func):
		keyByRDD = self.dataRDD.keyBy(func)
		tempObj = SparkRDD(data=keyByRDD)
		return tempObj

	def keys(self):
		keysRDD = self.dataRDD.keys()
		tempObj = SparkRDD(data=keysRDD)
		return tempObj

	def leftOuterJoin(self,toBeJoined):
		if isinstance(toBeJoined,SparkRDD):
			leftJoined = self.dataRDD.join(toBeJoined.dataRDD)
			tempObj = SparkRDD(data=leftJoined)
		else:
			#TODO: rasie err
			pass
		return tempObj


	def lookup(self,key):
		valuesList = self.dataRDD.lookup(key)
		return valuesList

	def mapPartitions(self,func):
		mapPartitionsRDD = self.dataRDD.mapPartitions(func)
		tempObj = SparkRDD(data=mapPartitionsRDD)
		return tempObj

	def mapPartitionsWithIndex(self,func):
		mapPartitionsWithIndexRDD = self.dataRDD.mapPartitionsWithIndex(func)
		tempObj = SparkRDD(data=mapPartitionsWithIndexRDD)
		return tempObj

	def mapPartitionsWithSplit(self,func):
		mapPartitionsWithSplitRDD = self.dataRDD.mapPartitionsWithSplit(func)
		tempObj = SparkRDD(data=mapPartitionsWithSplitRDD)
		return tempObj

	def mapValues(self,func):
		mapValuesRDD = self.dataRDD.mapValues(func)
		tempObj = SparkRDD(data=mapValuesRDD)
		return tempObj

	def max(self,func=None):
		if key in None:
			maxRDD = self.dataRDD.max()
		else:
			maxRDD = self.dataRDD.max(func)
		tempObj = SparkRDD(data=maxRDD)
		return tempObj

	def mean(self):
		meanRDD = self.dataRDD.mean()
		tempObj = SparkRDD(data=meanRDD)
		return tempObj

	def min(self,func=None):
		if func is None:
			minRDD = self.dataRDD.min()
		else:
			minRDD = self.dataRDD.min(func)
		tempObj = SparkRDD(data=minRDD)
		return tempObj

	def name(self):
		return self.dataRDD.name

	def reduceByKey(self,func=None):
		if key is None:
			reduceByKeyRDD = self.dataRDD.reduceByKey()
		else:
			reduceByKeyRDD = self.dataRDD.reduceByKey(func)
		tempObj = SparkRDD(data=reduceByKeyRDD)
		return tempObj

	def reduceByKeyLocally(self,func=None):
		if key is None:
			reduceByKeyLocallyRDD = self.dataRDD.reduceByKeyLocally()
		else:
			reduceByKeyLocallyRDD = self.dataRDD.reduceByKeyLocally(func)
		tempObj = SparkRDD(data=reduceByKeyLocallyRDD)
		return tempObj


	def rightOuterJoin(self,toBeJoined):
		if isinstance(toBeJoined,SparkRDD):
			rightOuterJoinRDD = self.dataRDD.join(toBeJoined.dataRDD)
			tempObj = SparkRDD(data=rightOuterJoinRDD)
		else:
			#TODO: rasie err
			pass
		return tempObj

	def sortBy(self,func,ascending=True):
		sortedBy = self.dataRDD.sortBy(func)
		sortedObj = SparkRDD(data=sortedBy)
		return sortedObj

	def sortByKey(self,func,ascending=True):
		sortedByKey = self.dataRDD.sortByKey(func)
		sortedObj = SparkRDD(data=sortedByKey)
		return sortedObj

	def substract(self,toBeSubtracted):
		if isinstance(toBeJoined,SparkRDD):
			subtracted = self.dataRDD.subtract(toBeSubtracted.dataRDD)
			subtractedObj = SparkRDD(data=subtracted)
		else:
			#TODO: raise err
			pass
		return subtractedObj

	def sum(self):
		summed = self.dataRDD.sum()
		sumObj = SparkRDD(data=summed)
		return sumObj

	def take(self,num):
		took = self.dataRDD.take(num)
		tookObj = SparkRDD(data=took)
		return tookObj

	def takeOrdered(self,num,func=None):
		if func is None:
			tookOredered = self.dataRDD.takeOrdered(num)
		else:
			tookOredered = self.dataRDD.takeOrdered(num,key=func)
		tookObj = SparkRDD(data=tookOredered)
		return tookObj

	def top(self,num,func):
		if func is None:
			topped = self.dataRDD.top(num)
		else:
			topped = self.dataRDD.top(num,key=func)
		toppedObj = SparkRDD(data=topped)
		return toppedObj

	def union(self):
		unioned = self.dataRDD.union()
		unionObj = SparkRDD(data=unioned)
		return unionObj

	def values(self):
		valued = self.dataRDD.values()
		valueObj = SparkRDD(data=valued)
		return valueObj

	def variance(self):
		return self.dataRDD.variance()


	def zip(self,toBeZipped):
		if isinstance(toBeZipped,SparkRDD):
			zipped = self.dataRDD.zip(toBeZipped)
		else:
			#TODO: Raise err
			pass
		zipObj = SparkRDD(data=zipped)
		return zipObj

	def zipWithIndex(self):
		zipped = self.dataRDD.zipWithIndex()
		zipObj = SparkRDD(data=zipped)
		return zipObj

	def zipWithUniqueId(self):
		zipped = self.dataRDD.zipWithUniqueId()
		zippedObj = SparkRDD(data = zipped)
		return zipObj
