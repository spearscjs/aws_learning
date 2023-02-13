# Import SparkSession
from pyspark.sql import SparkSession
import pyspark.sql.functions as psf

# Create SparkSession
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("SparkLearing") \
      .getOrCreate()

df=spark.read.parquet("s3://quintrix-spearscjs/data/src_customer/customer_details_parquet/")
df.show(10,False)
df.count()
# df=spark.read.csv("s3://quintrix-spearscjs/data/src_customer/customer_details_parquet/")
# df2=df.withColumn("col1",lit(10))
# df3=df.withColumn("col3",lit(30))
# df1.show(10,False)
# df2.write()
# df3.write()

# spark.sql(" set hive.e=true")


df=spark.read.parquet("s3://quintrix-spearscjs/data/src_customer/customer_details_parquet/")
df1=df.filter("account_id_type = 'Saving'")
df2=df1.withColumn('min_balance',psf.lit(20))
df3=df.filter("account_id_type != 'Saving'")
df4=df3.withColumn('min_balance',psf.lit(10))
df5=df4.unionAll(df2)



# --spark-submit  create.py
# df=spark.sql(""" select * from schema.table""")
df2.coalesce(2).write.mode('overwrite').parquet("s3://quintrix-spearscjs/data/src_customer/customer_details_parquet_write/load_date=2023-01-02 ")
df5.repartition('account_id').write.mode('overwrite').parquet("s3://aws-train-nov-de-data/data/src_customer/customer_details_parquet_write/")

spark.sql(""" inser overwrite table select a, b, case when () as d end from table 
  inner join aaaa""")

df=spark.read.parquet("s3://aws-train-nov-de-data/data/src_customer/customer_details_parquet/")
df.count()

spark.sql(""" select count(1) from schema.table """)

# Service Principal: glue.amazonaws.com is not authorized to perform:
# logs:PutLogEvents on resource: arn:aws:logs:us-east-1:341966982503:log-group:/aws-glue/crawlers:log-stream:src_customer-customer_details
# because no identity-based policy allows the logs:PutLogEvents action (Service: AWSLogs; Status Code: 400; Error Code: AccessDeniedException; Request ID: 0ae85245-d16f-4e93-9bfc-24bc5eff20a0; Proxy: null). For more information,
# see Setting up IAM Permissions in the Developer Guide (http://docs.aws.amazon.com/glue/latest/dg/getting-started-access.html).

spark.sql(""" select * from src_customer.customer_details_parquet_snappy  """).show(10,False)