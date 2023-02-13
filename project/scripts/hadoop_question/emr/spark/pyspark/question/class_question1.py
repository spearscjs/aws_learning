# 1. Run a spark job to load data into a s3
# 2. Build and Run a crawler to create table.
# 3. Run a spark job to load data into a s3 to a load_date folder
# 4. Build and Run a crawler to create table.  Check the partition.
# 5. Run the spark job to add another folder.load_date folder
# 6. Run crawler again to see the new metadata.
# 7. Add a new col in the datasets by changing the spark code. (see class_question2)
# 8. Run crawler again to see the new metadata.

from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("sourcedata").getOrCreate()

dataset_dates=[ "2022-01-01", "2022-01-02", "2022-01-03", "2022-01-04","2022-01-05","2022-01-06","2022-01-07","2022-01-08","2022-01-09","2022-01-10"]

# write tables to s3
for date_str in dataset_dates:
    df=spark.read.csv(f"s3://quintrix-spearscjs-landing/src_customer/cards_account_ingest_src/dataset_date={date_str}/cards_account_ingest_{date_str}.csv", header=True)
    df.write.parquet(f"s3://quintrix-spearscjs-landing/src_customer/cards_account_ingest/", partitionBy='dataset_date', mode='append')