# Import SparkSession
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("SparkLearing") \
      .getOrCreate()

# RDD Transformations
# Create RDD from parallelize
# flatMap(), map(), reduceByKey(), filter(), sortByKey()     --

dataList = [("Java", 20000), ("Python", 100000), ("Scala", 3000)]
rdd=spark.sparkContext.parallelize(dataList)

 # Read the below items
# RDD Action operation returns the values
# from an RDD to a driver node. In other words, any RDD function that returns non RDD[T] is considered as an action.
#
# Some actions on RDDs are count(), collect(), first(), max(), reduce() and more.