
''' 
1. Create a dag to have 4 task


Task 1. sensor
use date parameter from dag.
date=20230201
1. Check file exists in s3 bucket1/folder1/order_data_{date}.csv

Task 2:
2. Copy the file to another folder with
bucket1/folder2/2023-02-01/order_data_20230201.csv
bucket1/folder2/2023-02-02/order_data_20230202.csv

Task3:
3. Run the crawler to add the partition as dataset_date

Task4 .
Run a glue job (spark job)
get the total ammount by brand_name,product_name,dataset_date and load into file. Makse sure you have dynamic partition by brand_name
'''