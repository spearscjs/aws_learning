create table IF NOT EXISTS cards_ingest.account_src (
    account_id varchar(20),
    account_open_dt date,
    account_id_type varchar(10),
    acct_hldr_primary_addr_state varchar(2),
    acct_hldr_primary_addr_zip_cd varchar(20),
    acct_hldr_first_name varchar(100),
    acct_hldr_last_name varchar(100),
    dataset_date date);
