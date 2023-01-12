create table IF NOT EXISTS cards_ingest.order (
    order_id int encode delta, -- assumes unique, consecutive integer values. Because the differences are one byte, DELTA is a good choice. (see docs)
    brand_name varchar(20) encode text255,
    product_name varchar(20) encode text255,
    sales_ammount decimal(9,2) encode AZ64, -- or is raw better here because less overlap?
    sales_date date encode delta32k
) SORTKEY(sales_date);