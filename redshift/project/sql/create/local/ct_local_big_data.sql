create table IF NOT EXISTS cards_ingest.big_data (
    order_id int, 
    brand_name varchar(20),
    product_name varchar(20) ,
    sales_ammount decimal(9,2),
    sales_date date 
);