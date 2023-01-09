create table IF NOT EXISTS cards_ingest.cust_dim_details (
    cust_id varchar(10) encode raw,
    state_cd varchar(2) encode raw,
    zip_cd varchar(5) encode bytedict,
    cust_first_name varchar(20) encode raw,
    cust_last_name varchar(20) encode raw,
    start_date date encode delta32k,
    end_date date encode delta32k,
    active_flag varchar(1) encode raw
);