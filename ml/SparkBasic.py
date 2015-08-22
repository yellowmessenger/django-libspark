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

class SparkBasic():

	# When the SparkBasic is initialized - data and function
	# can be passed as arguments
	def __init__(self, data=[], **kwargs):
		self.func = kwargs.get("func",None)
		self.data = data
		try:
			self.dataRDD = sc.parallelize (data)
		except TypeError:
			self.dataRDD = data
		self.dataTF = None

	def map(self,func):
		mappedData = self.dataRDD.map(func)
		tempObj = SparkBasic(data = mappedData)
		return tempObj

	def filter(self,func):
		filteredData = self.dataRDD.filter(func)
		tempObj = SparkBasic(data = filteredData)
		return tempObj

	def collect(self):
		return self.dataRDD.collect()

	def count(self):
		return self.dataRDD.count()

	def first(self):
		return self.dataRDD.first()

	def flatMap(self,func):
		flatMapRDD = self.dataRDD.flatMap(func)
		tempObj = SparkBasic(data=flatMapRDD)
		return tempObj

	def flatMapValues(self,func):
		flatMapRDD = self.dataRDD.flatMapValues(func)
		tempObj = SparkBasic(data=flatMapRDD)
		return tempObj

	def groupBy(self,func):
		groupByRDD = self.dataRDD.groupBy(func)
		tempObj = SparkBasic(data=groupByRDD)
		return tempObj

	def groupByKey(self):
		groupByRDD = self.dataRDD.groupByKey()
		tempObj = SparkBasic(data=groupByRDD)
		return tempObj

	def intersection(self,toBeIntersected):
		if isinstance(toBeIntersected,SparkBasic):
			intersected = self.dataRDD.intersection(toBeIntersected.dataRDD)
			tempObj = SparkBasic(data=intersected)
		else:
			#TODO: rasie err
			pass
		return tempObj

	def join(self,toBeJoined):
		if isinstance(toBeJoined,SparkBasic):
			joined = self.dataRDD.join(toBeJoined.dataRDD)
			tempObj = SparkBasic(data=joined)
		else:
			#TODO: rasie err
			pass
		return tempObj

	def keyBy(self,func):
		keyByRDD = self.dataRDD.keyBy(func)
		tempObj = SparkBasic(data=keyByRDD)
		return tempObj

	def keys(self):
		keysRDD = self.dataRDD.keys()
		tempObj = SparkBasic(data=keysRDD)
		return tempObj

	def leftOuterJoin(self,toBeJoined):
		if isinstance(toBeJoined,SparkBasic):
			leftJoined = self.dataRDD.join(toBeJoined.dataRDD)
			tempObj = SparkBasic(data=leftJoined)
		else:
			#TODO: rasie err
			pass
		return tempObj


	def lookup(self,key):
		valuesList = self.dataRDD.lookup(key)
		return valuesList

	def mapPartitions(self,func):
		mapPartitionsRDD = self.dataRDD.mapPartitions(func)
		tempObj = SparkBasic(data=mapPartitionsRDD)
		return tempObj

	def mapPartitionsWithIndex(self,func):
		mapPartitionsWithIndexRDD = self.dataRDD.mapPartitionsWithIndex(func)
		tempObj = SparkBasic(data=mapPartitionsWithIndexRDD)
		return tempObj

	def mapPartitionsWithSplit(self,func):
		mapPartitionsWithSplitRDD = self.dataRDD.mapPartitionsWithSplit(func)
		tempObj = SparkBasic(data=mapPartitionsWithSplitRDD)
		return tempObj

	def mapValues(self,func):
		mapValuesRDD = self.dataRDD.mapValues(func)
		tempObj = SparkBasic(data=mapValuesRDD)
		return tempObj

	def max(self,func=None):
		if key in None:
			maxRDD = self.dataRDD.max()
		else:
			maxRDD = self.dataRDD.max(func)
		tempObj = SparkBasic(data=maxRDD)
		return tempObj

	def mean(self):
		meanRDD = self.dataRDD.mean()
		tempObj = SparkBasic(data=meanRDD)
		return tempObj

	def min(self,func=None):
		if func is None:
			minRDD = self.dataRDD.min()
		else:
			minRDD = self.dataRDD.min(func)
		tempObj = SparkBasic(data=minRDD)
		return tempObj

	def name(self):
		return self.dataRDD.name

	def reduceByKey(self,func=None):
		if key is None:
			reduceByKeyRDD = self.dataRDD.reduceByKey()
		else:
			reduceByKeyRDD = self.dataRDD.reduceByKey(func)
		tempObj = SparkBasic(data=reduceByKeyRDD)
		return tempObj

	def reduceByKeyLocally(self,func=None):
		if key is None:
			reduceByKeyLocallyRDD = self.dataRDD.reduceByKeyLocally()
		else:
			reduceByKeyLocallyRDD = self.dataRDD.reduceByKeyLocally(func)
		tempObj = SparkBasic(data=reduceByKeyLocallyRDD)
		return tempObj


	def rightOuterJoin(self,toBeJoined):
		if isinstance(toBeJoined,SparkBasic):
			rightOuterJoinRDD = self.dataRDD.join(toBeJoined.dataRDD)
			tempObj = SparkBasic(data=rightOuterJoinRDD)
		else:
			#TODO: rasie err
			pass
		return tempObj

	def sortBy(self,func,ascending=True):
		sortedBy = self.dataRDD.sortBy(func)
		sortedObj = SparkBasic(data=sortedBy)
		return sortedObj

	def sortByKey(self,func,ascending=True):
		sortedByKey = self.dataRDD.sortByKey(func)
		sortedObj = SparkBasic(data=sortedByKey)
		return sortedObj

	def substract(self,toBeSubtracted):
		if isinstance(toBeJoined,SparkBasic):
			subtracted = self.dataRDD.subtract(toBeSubtracted.dataRDD)
			subtractedObj = SparkBasic(data=subtracted)
		else:
			#TODO: raise err
			pass
		return subtractedObj

	def sum(self):
		summed = self.dataRDD.sum()
		sumObj = SparkBasic(data=summed)
		return sumObj

	def take(self,num):
		took = self.dataRDD.take(num)
		tookObj = SparkBasic(data=took)
		return tookObj

	def takeOrdered(self,num,func=None):
		if func is None:
			tookOredered = self.dataRDD.takeOrdered(num)
		else:
			tookOredered = self.dataRDD.takeOrdered(num,key=func)
		tookObj = SparkBasic(data=tookOredered)
		return tookObj

	def top(self,num,func):
		if func is None:
			topped = self.dataRDD.top(num)
		else:
			topped = sefl.dataRDD.top(num,key=func)
		toppedObj = SparkBasic(data=topped)
		return toppedObj

	def union(self):
		unioned = self.dataRDD.union()
		unionObj = SparkBasic(data=unioned)
		return unionObj

	def values(self):
		valued = self.dataRDD.values()
		valueObj = SparkBasic(data=valued)
		return valueObj

	def variance(self):
		return self.dataRDD.variance()


	def zip(self,toBeZipped):
		if isinstance(toBeZipped,SparkBasic):
			zipped = self.dataRDD.zip(toBeZipped)
		else:
			#TODO: Raise err
			pass
		zipObj = SparkBasic(data=zipped)
		return zipObj

	def zipWithIndex(self):
		zipped = self.dataRDD.zipWithIndex()
		zipObj = SparkBasic(data=zipped)
		return zipObj

	def zipWithUniqueId(self):
		zipped = self.dataRDD.zipWithUniqueId()
		zippedObj = SparkBasic(data = zipped)
		return zipObj


