''' 
Part1:

1. Create a python function to generate data : Sample data and store in a CSV file in you laptop.
The file name: order_data_20220301.csv
orderid,brand_name,product_name,sales_ammount,sales_date

brand_name:['Apple','Samsung','Nokia']
product_name is Key value pair
product_name:{'Apple':['iphon11','iphone12','iphone13','iphoneSE','IpadMax','IpadMini','laptop256','Macbook512'],
'Samsung':['galaxy10','galaxy11','galaxy12','galaxy13','watch320','watch340'],
'Nokia':['Nk320',''Nk400',''Nk500']
orderid: Unique id with 7 digit number

Finally data should be like this
1002346,Apple,iphon11,450,2022-01-06
1002246,Apple,iphon11,460,2022-02-01
1002546,Apple,iphoneSE,350,2022-03-06

'''

import pandas as pd
import random
import datetime


def random_date(start : datetime, end : datetime):
    diff = datetime.timedelta(days=(end - start).days)
    rae = random.random() * diff
    return start + datetime.timedelta(days = rae.days)


def generate_dummy_info(record_count, sales_date_min : datetime, sales_date_max : datetime):
    data = []
   

    for i in range(0, record_count):
        id_start = f"{i+1:07}"
        brand=random.choice(['Apple','Samsung','Nokia'])
        if brand=='Apple':
            product=random.choice(['iphon11','iphone12','iphone13','iphoneSE','IpadMax','IpadMini','laptop256','Macbook512'])
        elif brand=='Samsung':
            product=random.choice(['galaxy10','galaxy11','galaxy12','galaxy13','watch320','watch340'])
        else:
            product=random.choice(['Nk320','Nk400','Nk500'])
        sales_ammt = round(random.uniform(100.00, 999.99),2)
        sales_date = random_date(sales_date_min, sales_date_max)
        data.append((id_start, brand, product, sales_ammt, sales_date))

    return  pd.DataFrame(data, columns=['orderid', 'brand_name', 'product_name', 'sales_ammount', 'sales_date'])

if __name__ == '__main__':
    start_date = datetime.datetime(2022, 2, 1)
    end_date = datetime.datetime(2022, 2, 1)
    df = generate_dummy_info(50, start_date, end_date)
    print(df)