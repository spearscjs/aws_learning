COPY cards_ingest.order
FROM -- bucket path
credentials --CREDENTIAL GOES HERE-- 
csv IGNOREHEADER 1;