COPY cards_ingest.big_data 
FROM -- bucket path
credentials --CREDENTIAL GOES HERE-- 
csv IGNOREHEADER 1;