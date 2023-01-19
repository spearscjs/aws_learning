CREATE TABLE IF NOT EXISTS hadoop.tran_fact(
    tran_id varchar(18) encode raw, 
    cust_id varchar(20) encode raw,
    tran_ammount decimal(10,2) encode AZ64, 
    tran_type varchar(1) encode raw, 
    tran_country_cd varchar(3) encode bytedict,
    tran_date date encode delta32k
);