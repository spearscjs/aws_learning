-- Required parameter for dynamic partitions
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

insert into cards_dw.transaction_fact partition(dataset_date)
    select tran_id, cust_id, tran_ammount, tran_type, tran_country_cd,
        tran_date, CURRENT_DATE, dataset_date
    FROM src_customer.tran_fact
    WHERE dataset_date = '${hiveconf:dataset_date}';