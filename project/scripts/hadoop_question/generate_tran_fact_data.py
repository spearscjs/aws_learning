from utils import db
from utils import s3
from datetime import datetime, timedelta
import pandas as pd
import random


start_date = datetime(2023, 1, 1)
number_of_days = 7
record_count_min = 1000
record_count_max = 10000

tran_types = ['C', 'D']
state_cds = ['CA', 'AZ', 'NJ', 'AL', 'AK', 'CO', 'KY', 'WV']


def generate_dummy_info(record_count: int, date: datetime, start_id = 0):
    print(f'Generating {record_count} records for {date.strftime("%Y-%m-%d")}')
    ran_data = []
    for i in range(1, record_count+1):
        tran_id = f'{start_id + i:06}'
        cust_id = f'CA{random.randint(0,9999):04}'
        tran_ammount = round(random.uniform(100.00, 99999999.99),2)
        tran_type = random.choice(tran_types)
        stat_cd = random.choice(state_cds)
        tran_date = date.strftime("%Y-%m-%d")
        ran_data.append((tran_id, cust_id, tran_ammount, tran_type, stat_cd, tran_date))

    df = pd.DataFrame(ran_data, columns=['tran_id', 'cust_id', 'tran_ammount', 'tran_type', 'tran_stat_cd', 'tran_date'])
    filename = f'data/src_customer/tran_fact/daily/tran_fact_{date.strftime("%Y-%m-%d")}.csv'
    # df.to_parquet(filename, engine="fastparquet", )
    df.to_csv(filename, index=False)

    return filename


def generate_for_days(min_record_num: int, max_record_num: int, start_date: datetime, days: int):
    cur_date = start_date
    filenames = []
    count = 0
    for i in range(days):
        num_records = random.randint(min_record_num, max_record_num)
        filename = generate_dummy_info(num_records, cur_date, count)
        print(filename)
        filenames.append((filename, cur_date.strftime("%Y-%m-%d")))
        cur_date += timedelta(days=1)
        count += num_records
    print(f'Data generated for {days} days')
    return filenames

filenames = generate_for_days(record_count_min, record_count_max, start_date, number_of_days)


print(filenames)
# upload data to s3
for f in filenames:
    filename = f[0]
    name = f[0].split('/')[4]
    # add upload date for s3 folder names / hive partitions (tran_date plus one)
    date = list(f[1])
    date[9] = str((int(date[9]) + 1 ) % 10)
    date = "".join(date)
    s3.upload_file(filename, 'quintrix-spearscjs', f'data/src_customer/tran_fact/dataset_date={date}/{name}')
    print(filename, 'quintrix-spearscjs', f'data/src_customer/tran_fact/dataset_date={date}/{name}')