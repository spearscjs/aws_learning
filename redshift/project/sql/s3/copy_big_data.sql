COPY cards_ingest.big_data 
FROM 's3://quintrix-spearscjs/cards_ingest/order_data_20230401.csv'
credentials --CREDENTIAL GOES HERE-- 
csv IGNOREHEADER 1;