set hivevar:src_schema=src_customer;
use ${hivevar:src_schema};

create table if not exists customer_details_parquet_snappy
(
account_id varchar(50),
account_open_dt varchar(50),
account_id_type varchar(10),
acct_hldr_primary_addr_state varchar(20),
acct_hldr_primary_addr_zip_cd varchar(20),
acct_hldr_first_name varchar(20),
acct_hldr_last_name varchar(20),
dataset_date varchar(50))
stored as parquet
TBLPROPERTIES ("parquet.compression"="SNAPPY");
;

DESCRIBE FORMATTED customer_details_parquet_snappy ;

insert into customer_details_parquet_snappy select * from customer_details_parquet;
