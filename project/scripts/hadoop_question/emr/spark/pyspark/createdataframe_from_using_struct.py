# Import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType ,StructField, StringType, IntegerType
# from pyspark.sql.functons as psf

# Create SparkSession
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("SparkLearing") \
      .getOrCreate()




data2 = [("Jamesdsfwegererthrthrthrta" ,"" ,"Smith" ,"36636" ,"M" ,3000),
         ("Micfwqefefwefwefewhael" ,"Rose" ,"" ,"40288" ,"M" ,4000),
         ("Robert" ,"" ,"Williams" ,"42114" ,"M" ,4000),
         ("Maria" ,"Anne" ,"Jones" ,"39192" ,"F" ,4000),
         ("Jen" ,"Mary" ,"Brown" ,"" ,"F" ,-1)
         ]

schema = StructType([ \
    StructField("firstname" ,StringType() ,True),
    StructField("middlename" ,StringType() ,True),
    StructField("lastname" ,StringType() ,True),
    StructField("id", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("salary", IntegerType(), True)
    ])

df = spark.createDataFrame(data=data2 ,schema=schema)
df.printSchema()
df.show(truncate=False)