create table IF NOT EXISTS cards_ingest.product_cost (
    product_name varchar(20) encode raw, -- raw because small table
    manufacturing_cost decimal(5,2) encode raw --  raw because small table
);