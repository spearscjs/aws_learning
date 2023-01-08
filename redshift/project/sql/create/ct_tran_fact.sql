/* see https://docs.aws.amazon.com/redshift/latest/dg/Examples__compression_encodings_in_CREATE_TABLE_statements.html*/
CREATE TABLE IF NOT EXISTS cards_ingest.tran_fact(
    tran_id int encode delta,
    cust_id varchar(10) encode raw, 
    stat_cd varchar(2) encode raw, 
    tran_ammt decimal(10,2) encode AZ64,
    tran_date date encode delta
) SORTKEY(tran_date);