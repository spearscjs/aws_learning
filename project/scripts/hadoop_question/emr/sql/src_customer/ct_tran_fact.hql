create table if not exists src_customer.tran_fact
(
    tran_id varchar(18) , 
    cust_id varchar(20) ,
    tran_ammount decimal(10,2), 
    tran_type varchar(1) , 
    tran_country_cd varchar(3) ,
    tran_date date
)
partitioned by (dataset_date varchar(10))
row format delimited fields terminated by ','
location "s3://quintrix-spearscjs/data/src_customer/tran_fact/"
tblproperties ("skip.header.line.count"="1") ;