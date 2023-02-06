
ssh -i sanjeeb_ec2.pem  hadoop@ec2-44-201-173-164.compute-1.amazonaws.com

type : hive to open hive shell
or
export AWS_PROFILE=dev_acct
--beeline -u jdbc:hive2://ec2-44-201-173-164.compute-1.amazonaws.com:10000/default
aws emr terminate-clusters --cluster-ids j-3CPJ1GS4TEC85

show databases;

--hdfs dfs -chown -R hive /user/data

-- This has to be doe ina diferent shell
hdfs dfs -ls /user/
hdfs dfs -mkdir -p /user/data/src_customer     >> will hdfs directory

create database src_customer
location "/user/data/src_customer";

--set src_schema='src_customer';
--use ${hiveconf:src_sche



use src_customer;
show tables;

set hivevar:src_schema=src_customer;
use ${hivevar:src_schema};


create table if not exists customer_details
(
account_id varchar(50),
account_open_dt varchar(50),
account_id_type varchar(10),
acct_hldr_primary_addr_state varchar(20),
acct_hldr_primary_addr_zip_cd varchar(20),
acct_hldr_first_name varchar(20),
acct_hldr_last_name varchar(20),
dataset_date varchar(50))
row format delimited fields terminated by ','
location "/user/data/src_customer/customer_details/"
tblproperties ("skip.header.line.count"="1")
;


stored as parquet;

insert into table 1111 as select * from table000;

hdfs dfs -cp s3://aws-train-nov-de/cards_ingest/account_src/cards_account_ingest_2022-01-02.csv /user/data/src_customer/customer_details/

set hive.cli.print.header=true;
select * from customer_details limit 5;