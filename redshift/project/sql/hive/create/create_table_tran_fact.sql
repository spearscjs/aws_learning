/* HIVE COMMANDS */ 
create database cards_ingest
location "s3://quintrix-spearscjs/data/cards_ingest"

set hivevar:src_schema=cards_ingest;
use ${hivevar:src_schema};

/*

create tran_fact with parititons

*/
create table if not exists tran_fact
(
    tran_id varchar(18) , 
    cust_id varchar(20) ,
    tran_ammount decimal(10,2), 
    tran_type varchar(1) , 
    tran_country_cd varchar(3) ,
    tran_date date
)
partitioned by (load_date varchar(10))
row format delimited fields terminated by ','
location "s3://quintrix-spearscjs/data/cards_ingest/tran_fact/daily/"
tblproperties ("skip.header.line.count"="1") ;


/* BASH COMMANDS */


alter table tran_fact add partition (load_date='2022-01-01');
alter table tran_fact add partition (load_date='2022-01-02');
alter table tran_fact add partition (load_date='2022-01-03');
alter table tran_fact add partition (load_date='2022-01-04');
alter table tran_fact add partition (load_date='2022-01-05');
alter table tran_fact add partition (load_date='2022-01-06');
alter table tran_fact add partition (load_date='2022-01-07');

aws s3 cp s3://quintrix-spearscjs/cards_ingest/tran_fact/daily/2022-01-01/tran_fact_2022-01-01.csv s3://quintrix-spearscjs/data/cards_ingest/tran_fact/daily/load_date='2022-01-01'/
aws s3 cp s3://quintrix-spearscjs/cards_ingest/tran_fact/daily/2022-01-02/tran_fact_2022-01-02.csv s3://quintrix-spearscjs/data/cards_ingest/tran_fact/daily/load_date='2022-01-02'/
aws s3 cp s3://quintrix-spearscjs/cards_ingest/tran_fact/daily/2022-01-03/tran_fact_2022-01-03.csv s3://quintrix-spearscjs/data/cards_ingest/tran_fact/daily/load_date='2022-01-03'/
aws s3 cp s3://quintrix-spearscjs/cards_ingest/tran_fact/daily/2022-01-04/tran_fact_2022-01-04.csv s3://quintrix-spearscjs/data/cards_ingest/tran_fact/daily/load_date='2022-01-04'/
aws s3 cp s3://quintrix-spearscjs/cards_ingest/tran_fact/daily/2022-01-05/tran_fact_2022-01-05.csv s3://quintrix-spearscjs/data/cards_ingest/tran_fact/daily/load_date='2022-01-05'/
aws s3 cp s3://quintrix-spearscjs/cards_ingest/tran_fact/daily/2022-01-06/tran_fact_2022-01-06.csv s3://quintrix-spearscjs/data/cards_ingest/tran_fact/daily/load_date='2022-01-06'/
aws s3 cp s3://quintrix-spearscjs/cards_ingest/tran_fact/daily/2022-01-07/tran_fact_2022-01-07.csv s3://quintrix-spearscjs/data/cards_ingest/tran_fact/daily/load_date='2022-01-07'/


select count(1),load_date from tran_fact_temp group by load_date ;
