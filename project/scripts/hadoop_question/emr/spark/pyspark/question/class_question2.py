# Build Pyspark job to do this.
# 1. Read the transaction csv file . (10 files 1 by 1)
# 2. Add load_time  (current_time)
# 3. concat first_name and last name along with space in between. The new col is name.
# 4. get the first two character of account id. if its CK the create a new field account_type as Checking
# if its PV then Private and else saving.
# 5. Drop the account_id_type col from dataset.
# 6. Load data into a new folder with customer_pyspark and each day add a partition of the day .
# lets say you are running for 2023-01-01 then partition is same

# USAGE #########################################################################
# spark-submit <dataset_date> [...dataset_date] 
# EX: spark-submit class_question2.py 2022-01-01 2022-01-02 2022-01-03 2022-01-04 2022-01-05 2022-01-06 2022-01-07 2022-01-08 2022-01-09 2022-01-10

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, concat_ws, when, substring
import sys

"""
    transform spark dataframe

    :param df: spark dataframe 
    :return: spark DataFrame to transform
    :rtype: spark DataFrame
    """
def transform(df):
    df = df.withColumn("load_time", current_timestamp())
    df = df.withColumn("name", concat_ws(" ", df.acct_hldr_first_name, df.acct_hldr_last_name))
    df = df.withColumn("account_type", 
        when(substring(df.account_id, 1, 2) == "CK", "Checking")
        .when(substring(df.account_id, 1, 2) == "PV", "Private")
        .otherwise("Saving")                   
    )
    return df


if __name__ == "__main__":
    spark = SparkSession.builder.master("local").appName("Spark_Learning").getOrCreate()
    # skip first argument (argv[0] is file for spark-submit [filename])
    for date_str in sys.argv[1:]:

        src_loc = f"s3://quintrix-spearscjs-landing/cards_ingest/cards_account_ingest_src/dataset_date={date_str}/cards_account_ingest_{date_str}.csv"
        dest_loc = f"s3://quintrix-spearscjs-transformed/src_customer/customer_pyspark/"
       
        # read in csv data to transform
        df=spark.read.csv(src_loc, header=True)
        df = transform(df)
        df = df.drop("account_id_type")
        df.write.parquet(dest_loc, partitionBy='dataset_date', mode='append')

    spark.stop()
