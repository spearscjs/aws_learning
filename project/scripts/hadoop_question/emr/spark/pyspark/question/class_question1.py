# USAGE:
#   spark-submit class_question1.py <dataset_date> [...dataset_date] 
#     EX: spark-submit class_question1.py 2022-01-01 2022-01-02 2022-01-03 2022-01-04 2022-01-05 2022-01-06 2022-01-07



from pyspark.sql import SparkSession
from pyspark.sql.functions import count, rank, desc, when
from pyspark.sql.window import Window
import sys
spark = SparkSession.builder.master("local").enableHiveSupport().appName("SparkPractice").getOrCreate()


# 1. Run a spark job to load data into a s3
# 2. Build and Run a crawler to create table.
# 3. Run a spark job to load data into a s3 to a load_date folder
# 4. Build and Run a crawler to create table.  Check the partition.
# 5. Run the spark job to add another folder.load_date folder
# 6. Run crawler again to see the new metadata.
# 7. Add a new col in the datasets by changing the spark code. (see class_question2)
# 8. Run crawler again to see the new metadata.
 

# write tables to s3

for date_str in sys.argv[1:]:
    df=spark.read.csv(f"s3://quintrix-spearscjs/data/src_customer/cards_account_ingest_src/dataset_date={date_str}/cards_account_ingest_{date_str}.csv", header=True)
    df.write.parquet(f"s3://quintrix-spearscjs/data/src_customer/cards_account_ingest/", partitionBy='dataset_date', mode='append')

df.show(10)


# df=Read 1 week data
# 1. Get the total number of account_id by account_open_dt as count_accont_by_date. [here you will get 7 record]
# spark-submit class_question2.py 2022-01-01 2022-01-02 2022-01-03 2022-01-04 2022-01-05 2022-01-06 2022-01-07 2022-01-08 2022-01-09 2022-01-10
paths = []
for date_str in sys.argv[1:]:
  paths.append(f"s3://quintrix-spearscjs/data/src_customer/customer_pyspark/dataset_date={date_str}")
df=spark.read.format("parquet").option("basePath", f"s3://quintrix-spearscjs/data/src_customer/customer_pyspark/").load(paths)
df.show(10)


# group by account_open_dt
# df_CABD = df.groupBy(df.account_open_dt).agg(count(df.account_id)).withColumnRenamed('count', 'count_account_by_date').show(10)
# df_CABD.show()

# group by dataset_date
df_CABD = df.groupBy(df.dataset_date).agg(count(df.account_id)).withColumnRenamed('count(account_id)', 'count_account_by_date')
df_CABD.show()



# 2. Get the total number of account_id by account_type as count_accont_by_type   [ here you will get three record]


#spark.sql("SELECT dataset_date, account_type, count(account_id) AS count_accuont_by_type FROM src_customer.customer_pyspark GROUP BY dataset_date").show()
#customer_pyspark.show(10)
# group by dataset_date
df_CABT = df.groupBy(df.account_type).agg(count(df.account_id)).withColumnRenamed('count(account_id)', 'count_account_by_type')
df_CABT.show(10)


# 3. Create field --account_stats -- values[ 'First','Second','Third'] by ranking from step2. [ here you will get three record]
windowSpec  = Window.orderBy(desc(df_CABT.count_account_by_type))
df_rank = df_CABT.withColumn("rank", rank().over(windowSpec))
df_stats = df_rank.withColumn("account_stats", 
      when(df_rank.rank == 1, "First")
      .when(df_rank.rank == 2, "Second")
      .otherwise("Third"))

df_stats = df_stats.drop('rank')
df_stats.show()






# Joins Task (Inner)
# Now . join df with step1 on account_open_dt get the count_accont_by_date
df_step1 = df.join(df_CABD, df.dataset_date == df_CABD.dataset_date, "inner").select(df['*'], df_CABD.count_account_by_date)
# Join step2 on account_type get count_accont_by_type
df_step2 = df_step1.join(df_CABT, df_step1.account_type == df_CABT.account_type, "inner").select(df_step1['*'], df_CABT.count_account_by_type) 
# Join step3 on account_type get account_stats
df_step3 = df_step2.join(df_stats, df_step2.account_type == df_stats.account_type, "inner").select(df_step2['*'], df_stats.account_stats)

df_step3.show()

# load data into dynamic partition table by dataset_date account_stats
df_step3.write.parquet(f"s3://quintrix-spearscjs/data/src_customer/customer_pyspark_stats/", partitionBy='dataset_date', mode='append')
