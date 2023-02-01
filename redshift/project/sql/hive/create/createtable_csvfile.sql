

ls -ltr  >>> Lis file in a directory
cp /path/file_name  /path1/  >> copying file from path to path1

hdfs dfs -ls

Pat1:

1. Create a python function (Script) to generate data : Sample data and store in a CSV file in you laptop. [50000 records]
The file name: order_data_20220401.csv


orderid,brand_name,product_name,sales_ammount,sales_date
orderid: Unique id with 7 digit number in sequence[Pandas function random number]
brand_name:['Apple','Samsung','Nokia']
product_name is Key value pair
product_name:{'Apple':['iphon11','iphone12','iphone13','iphoneSE','IpadMax','IpadMini','laptop256','Macbook512'],
'Samsung':['galaxy10','galaxy11','galaxy12','galaxy13','watch320','watch340'],
'Nokia':['Nk320',''Nk400',''Nk500']
sales_ammount : random ammout with (5,2) decimal.
sales_date : 2022-01-01 to 2022-03-320221

Finally data should be like this
1002346,Apple,iphon11,450,2022-01-06
1002246,Apple,iphon11,460,2022-02-01
1002546,Apple,iphoneSE,350,2022-03-06

Use this dataset in s3.

Create all managed table.

1. Create a hive table with CSV file.
2. Create hive table with CSV file but "|" delimited
3. Create a hive table with parquet format.
4.