

-- 1. Find out all the cust id where the previous (last only) transaction is more then current transaction ammount?

-- think: tran_date can happen at the same time
-- grain what is the grain of the data (can one transaction)
-- WRONG -- need to compare last transaction to total by time (some trans happen at same time)
WITH prev_ammts as 
    (SELECT *, LAG(tran_ammt) OVER (PARTITION BY cust_id ORDER BY tran_date) AS  prev_tran_ammt
    FROM cards_ingest.tran_fact)
SELECT cust_id, tran_id, prev_tran_ammt, tran_ammt FROM prev_ammts WHERE tran_ammt > prev_tran_ammt

SELECT * FROM cards_ingest.tran_fact;


-- 2. Show the cust id, state cd, total trsaction ammount till date (running total) for all the records.
SELECT *, 
    SUM(tran_ammt) OVER (PARTITION BY cust_id ORDER BY tran_date, tran_ammt) 
FROM cards_ingest.tran_fact;


-- 3. Show all the cust id,state cd, total number of trasaction (Include all the transactio per state cd) and
-- total_transaction ammont per state cd, total number of trasaction (don't include any transaction which are less than 1000)
WITH totals as 
    (SELECT *, 
        COUNT(*) OVER (PARTITION BY cust_id,stat_cd) AS tran_count 
    FROM cards_ingest.tran_fact
    WINDOW w as (PARTITION BY cust_id,stat_cd))
SELECT DISTINCT cust_id, stat_cd, tran_count, SUM(tran_ammt) OVER (PARTITION BY cust_id,stat_cd) AS tran_sum FROM totals 
GROUP BY cust_id, stat_cd, tran_ammt, tran_count



-- Note : Here in case 1 you are including all the transaction to show the count but later you have tu just include only where >1000
-- 4. in cust_dim_details change all the name from Mike to Nike where cust_last_name !='dogeee'
UPDATE cards_ingest.cust_dim_details
SET
    cust_first_name = 'Nike'
WHERE
    cust_last_name IS NULL OR (cust_last_name != 'doge' AND cust_first_name ='Mike');

SELECT * FROM cards_ingest.cust_dim_details


