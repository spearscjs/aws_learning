# Import SparkSession
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("SparkLearning") \
      .getOrCreate()


# DataFrame is a distributed collection of data organized into named columns.
# It is conceptually equivalent to a table in a relational database or a data frame in R/Python,
# but with richer optimizations under the hood. DataFrames can be constructed from
# a wide array of sources such as structured data files, tables in Hive, external databases, or existing RDDs.

# using createDataFrame()
data = [('James','','Smith','1991-04-01','M',3000),
  ('Michael','Rose','','2000-05-19','M',4000),
  ('Robert','','Williams','1978-09-05','M',4000),
  ('Maria','Anne','Jones','1967-12-01','F',4000),
  ('Jen','Mary','Brown','1980-02-17','F',-1)
]

columns = ["firstname","middlename","lastname","dob","gender","salary"]
df = spark.createDataFrame(data=data, schema = columns)
df.count()

df.show()

# DataFrame operations
# Like RDD, DataFrame also has operations like Transformations and Actions.
#
# DataFrame from external data sources
# In real-time applications, DataFrames are created from external sources like files from the local
# system, HDFS, S3 Azure, HBase, MySQL table e.t.c. Below is an example of how to read a CSV file from a local system.

columns = ["language","users_count"]
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]

spark = SparkSession.builder.appName('rddd.com').getOrCreate()
rdd = spark.sparkContext.parallelize(data)
# Using toDF() function
dfFromRDD1 = rdd.toDF()
dfFromRDD1.printSchema()


columns = ["language","users_count"]
dfFromRDD1 = rdd.toDF(columns)
dfFromRDD1.printSchema()