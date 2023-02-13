create table if not exists src_customer.table_states (
    load_date date,
    rec_count int
)
partitioned by (database_name varchar(20), table_name varchar(50), partition_key varchar(30))
stored as parquet
location "s3://quintrix-spearscjs/data/src_customer/table_states/";