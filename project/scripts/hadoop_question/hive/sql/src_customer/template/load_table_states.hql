
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT INTO src_customer.table_states partition(database_name,table_name,partition_key)
SELECT CURRENT_DATE, count(*), 'src_customer','tran_fact', dataset_date
FROM src_customer.tran_fact  where dataset_date='${dataset_date}' GROUP BY dataset_date;
