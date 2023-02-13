# %%
import numpy as np
import pandas as pd
import random
from random import randint
import string
from pyspark.sql.types import *
from pyspark.sql.functions import when
from pyspark.ml.feature import Imputer
from datetime import datetime, timedelta
from pyspark import SparkContext

# %%
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("sourcedata").getOrCreate()

# %%


# %%
def getRandomDates():
    start = datetime.now()
    end = start + timedelta(days=700)
    random_date = (start - (end - start) * random.random()).strftime('%Y-%m-%d')
    return random_date

# %%
def get_random_string(length):
    # choose from all lowercase letter
    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str

# %%
def build_schema(dataset_date):
    print("Build and return a schema to use for the sample data.")
    schema = StructType(
        [
            StructField("account_id",  StringType(),  True),
            StructField("account_open_dt",      StringType(),  True),
            StructField("account_id_type", StringType(),  True),
            StructField("acct_hldr_primary_addr_state", StringType(),  True),
            StructField("acct_hldr_primary_addr_zip_cd",    StringType(),  True),
            StructField("acct_hldr_first_name", StringType(),  True),
            StructField("acct_hldr_last_name", StringType(),  True),
            StructField("dataset_date",  StringType(),  True)
        ]
    )
    return schema

# %%
from pyspark import SparkContext 
def build_data(dataset_date, cnt):
    rec = []
    for i in range(1,rec_cnt):
        account_id =  random.choice(["SV","CK","PV"])+str(randint(27618,189765))
        account_open_dt = getRandomDates()
        acct_hldr_primary_addr_zip_cd=str(randint(23456,98776))
        acct_hldr_primary_addr_state = random.choice(['AL','AK','AZ','AR','CA','CO','CT','DE','DC',\
                                                      'FL','GA','HI','ID','IL','IN','IA','KS','KY','LA',\
                                                      'ME','MD','MA'])
        account_id_type = random.choice(["Saving","Checkin","Private"])
        acct_hldr_first_name=get_random_string(10)
        acct_hldr_last_name=get_random_string(6)
        rec.append(account_id + "~" +
                   account_open_dt + "~" +
                   account_id_type + "~" +
                   acct_hldr_primary_addr_state + "~" +
                   acct_hldr_primary_addr_zip_cd + "~" +
                   acct_hldr_first_name + "~" +
                   acct_hldr_last_name + "~" +
                   dataset_date)
    return (rec)





import os 
rec_cnt=1001
dataset_dates=["2022-01-01","2022-01-02","2022-01-03","2022-01-04","2022-01-05","2022-01-06","2022-01-07","2022-01-08","2022-01-09","2022-01-10"]
for d in dataset_dates:
    data_dir=f"data/cards_ingest/cards_account_ingest/dataset_date={d}/"
    if(not os.path.exists(data_dir)):
        os.makedirs(data_dir)
    file_name=data_dir+d + ".csv"
   
    
    columns=['account_id','account_open_dt','account_id_type','acct_hldr_primary_addr_state',\
                'acct_hldr_primary_addr_zip_cd','acct_hldr_first_name','acct_hldr_last_name','dataset_date']
    sc = spark.sparkContext
    hist_data = build_data(d, int(rec_cnt))
    histrawInput = sc.parallelize(hist_data)
    histrawInputSplit = histrawInput.map(lambda x: x.split("~"))
    hist_df = spark.createDataFrame(histrawInputSplit, schema=build_schema(d))
    hist_df.toPandas().to_csv(file_name,index=False)
  
#






# %%
# hist_df.toPandas().to_csv(file_name,index=False)

# %%



