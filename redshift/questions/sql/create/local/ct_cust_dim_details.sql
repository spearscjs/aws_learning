create table IF NOT EXISTS cards_ingest.cust_dim_details (
    cust_id varchar(10),
    state_cd varchar(2) ,
    zip_cd varchar(5) ,
    cust_first_name varchar(20) ,
    cust_last_name varchar(20) ,
    start_date date ,
    end_date date ,
    active_flag varchar(1)
);