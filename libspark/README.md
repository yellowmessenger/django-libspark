Django Spark Lib
================

This is the core Django Spark Lib. This library is built on top of [PySpark](https://spark.apache.org/docs/0.9.0/python-programming-guide.html) available for [Apache Spark](http://spark.apache.org/).

What makes Spark Lib easy to use?
=================================

Spark Lib makes communication to a Spark instacnce a cake walk. Spark Lib takes care of all the connection related initialisation scripts. It also supports powerfull method chaining mechanism along with lazy evaluation.

Packages
========

1. [pyspark.RDD](http://spark.apache.org/docs/latest/api/python/pyspark.html#pyspark.RDD)

Functions Implemented
=====================

* map
* filter
* collect
* cache
* count
* distinct
* first
* flatMap
* flatMapValues
* groupBy
* groupByKey
* intersection
* join
* keyBy
* keys
* leftOuterJoin
* lookup
* mapPartitions
* mapPartitionsWithIndex
* mapPartitionsWithSplit
* mapValues
* max
* mean
* min
* name
* reduceByKey
* reduceByKeyLocally
* rightOuterJoin
* sortBy
* sortByKey
* substract
* sum
* take
* takeOrdered
* top
* union
* values
* variance
* zip
* zipWithIndex
* zipWithUniqueId
