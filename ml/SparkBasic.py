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
		#TODO: check type of toBeIntersected
		if isinstance(toBeIntersected,SparkBasic):
			intersected = self.dataRDD.intersection(toBeIntersected.dataRDD)
			tempObj = SparkBasic(data=intersected)
		else:
			#TODO: rasie err
			pass
		return tempObj