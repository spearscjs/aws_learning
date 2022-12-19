
/*
cards_ingest.tran_fact
cards_ingest.cust_dim_details
*/


-- 1. Calculate total tran_ammt (sum) for each state
SELECT stat_cd, SUM(tran_ammt) AS total_tran_ammt FROM cards_ingest.tran_fact GROUP BY stat_cd;


-- 2. Calculate maximum and minimum tran_ammt on each state and tran_date
SELECT stat_cd, MAX(tran_ammt) AS max_tran_ammt, MIN(tran_ammt) AS min_tran_ammt 
FROM cards_ingest.tran_fact 
GROUP BY stat_cd;


-- 3. Calculate total transaction which have tran_ammt more than 10000
SELECT tran_id, tran_ammt FROM cards_ingest.tran_fact WHERE tran_ammt > 10000;


-- 4. Show the state which have total (sum) tran_ammt more than 10000
SELECT stat_cd, SUM(tran_ammt) AS total_tran_ammt 
FROM cards_ingest.tran_fact 
GROUP BY stat_cd 
HAVING SUM(tran_ammt) > 10000;


-- 5. show me the states where total ammt is more than 10000
SELECT stat_cd, SUM(tran_ammt) AS total_tran_ammt FROM cards_ingest.tran_fact GROUP BY stat_cd HAVING SUM(tran_ammt) > 10000;


-- 6. show me the states where cust_id ='cust_104' and  total ammt is more than 10000
SELECT stat_cd
FROM cards_ingest.tran_fact 
WHERE cust_id ='cust_104'  
GROUP BY stat_cd 
HAVING SUM(tran_ammt) > 10000;


-- 7. Calculate total transaction by state [ if state if NULL make it TX] where total transaction is more than 10000
SELECT 
	CASE 
		WHEN stat_cd IS NULL 
			THEN 'TX' 
		ELSE stat_cd
	END temp_stat_cd, 
	SUM(tran_ammt) AS total_tran_ammt 
FROM cards_ingest.tran_fact 
GROUP BY temp_stat_cd 
HAVING SUM(tran_ammt) > 10000;


-- 8. Show me a message col if state is null then "missing data" else "good data"
SELECT stat_cd,
	CASE
		WHEN stat_cd IS NULL
			THEN 'missing data'
		ELSE 'good data'
	END 
	AS "message"
FROM cards_ingest.tran_fact;


-- 9. Show me sum of tran_ammt by state [ if state is null and cust_id='cust_104' then 'TX' else 'CA']
WITH temp_tran_fact AS
	(SELECT 
		CASE 
			WHEN stat_cd IS NULL AND cust_id='cust_104' 
				THEN 'TX' 
			WHEN stat_cd IS NULL 
				THEN 'CA'
			ELSE stat_cd
		END, tran_ammt
	FROM cards_ingest.tran_fact) 
SELECT stat_cd, SUM(tran_ammt) AS total_tran_ammt FROM temp_tran_fact GROUP BY stat_cd;



-- Join Question:

-- 1.Give me all details from transaction tale and zip_cd from dimension table.
SELECT tran_fact.*, zip_cd
FROM cards_ingest.tran_fact tran_fact 
JOIN cards_ingest.cust_dim_details dim_details
ON tran_fact.cust_id = dim_details.cust_id;


-- 2. Sum of tran_ammt by zip_cd 
SELECT COALESCE(zip_cd, '001') AS zip_cd, SUM(tran_ammt) AS total_tran_ammt FROM cards_ingest.cust_dim_details AS dim_details
JOIN cards_ingest.tran_fact tran_fact 
ON dim_details.cust_id = tran_fact.cust_id
GROUP BY zip_cd;


-- 3. Give me top 5 customer [ (first name+ last name) is customer] by tran_ammt [highest is first] join on cust_id
-- below query assumes each cust_id is a seperate customer
SELECT CONCAT(cust_first_name, ' ', cust_last_name)
FROM cards_ingest.tran_fact tran_fact 
JOIN cards_ingest.cust_dim_details dim_details
ON tran_fact.cust_id = dim_details.cust_id
GROUP BY tran_fact.cust_id, cust_first_name, cust_last_name 
ORDER BY SUM(tran_ammt) DESC LIMIT 5;

-- below query treats first_name + last_name as same customer 
SELECT CONCAT(cust_first_name, ' ', cust_last_name)
FROM cards_ingest.tran_fact tran_fact 
JOIN cards_ingest.cust_dim_details dim_details
ON tran_fact.cust_id = dim_details.cust_id
GROUP BY cust_first_name, cust_last_name 
ORDER BY SUM(tran_ammt) DESC LIMIT 5;


-- 4. Give me the all cols from tran_fact [ I don't need state_cd is null] first five records [ lower to highest]
-- i am confused as to what this i asking
-- ?????
SELECT * FROM cards_ingest.tran_fact tran_fact WHERE stat_cd IS NOT NULL ORDER BY tran_ammt LIMIT 5;