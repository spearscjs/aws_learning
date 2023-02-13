create table if not exists cards_dw.transaction_fact
(
    tran_id varchar(18) , 
    cust_id varchar(20) ,
    tran_ammount decimal(10,2), 
    tran_type varchar(1) , 
    tran_country_cd varchar(3) ,
    tran_date date,
    load_time date 
)
partitioned by (dataset_date varchar(10))
stored as parquet
location "s3://quintrix-spearscjs/data/cards_dw/transaction_fact/"
TBLPROPERTIES ("parquet.compression"="SNAPPY");