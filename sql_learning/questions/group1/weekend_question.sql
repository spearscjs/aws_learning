-- Questions:

-- Part 1
-- 1. show me all the tran_date,tran_ammt and total transaction ammount per tran_date
SELECT tran_date, tran_ammt,  
	SUM(tran_ammt) OVER (PARTITION BY tran_date) AS total_tran_per_date 
FROM cards_ingest.tran_fact;



-- 2. show me all the tran_date,tran_ammt and total transaction ammount per tran_date and rank of the transaction ammount desc within per tran_date
/*
Ouput:
2022-01-01,7145.00,19543.00,1
2022-01-01,6125.00,19543.00,2
*/
SELECT tran_date, tran_ammt,  
	SUM(tran_ammt) OVER w AS total_tran_per_date, 
	RANK() OVER (w ORDER BY tran_ammt DESC) 
FROM cards_ingest.tran_fact
WINDOW w AS (PARTITION BY tran_date);



-- 3. show me all the fields and total tansaction ammount per tran_date and only 2nd rank of the transaction ammount desc within per tran_date
--  (Here you are using he question2 but filtering only for rank 2)
SELECT * FROM
(
	SELECT *,
		SUM(tran_ammt) OVER w AS total_tran_per_date, 
		RANK() OVER (w ORDER BY tran_ammt DESC) r
	FROM cards_ingest.tran_fact
	WINDOW w AS (PARTITION BY tran_date)
) t_ammts
WHERE r = 2;



-- Part 2

-- 1. Join tran_fact and cust_dim_details on cust_id and tran_dt between start_date and end_date
SELECT tran_id tran_ammt, tran_date, dd.* 
FROM cards_ingest.tran_fact tf
JOIN cards_ingest.cust_dim_details dd
	ON tf.cust_id = dd.cust_id 
	AND (
		tf.tran_date >= dd.start_date 
		AND tf.tran_date <= dd.end_date 
	);



-- 2. show me all the fields and total tansaction ammount per tran_date and only 2nd rank of the transaction
--  ammount desc within per tran_date(Here you are using he question2 but filtering only for rank 2) and join
--   cust_dim_details on cust_id and tran_dt between start_date and end_date
SELECT tran_id tran_ammt, tran_date, dd.* 
FROM 
(
	SELECT *,
		SUM(tran_ammt) OVER w AS total_tran_per_date, 
		RANK() OVER (w ORDER BY tran_ammt DESC) r
	FROM cards_ingest.tran_fact
	WINDOW w AS (PARTITION BY tran_date)
) tf
JOIN cards_ingest.cust_dim_details dd
	ON tf.cust_id = dd.cust_id 
	AND (
		tf.tran_date >= dd.start_date 
		AND tf.tran_date <= dd.end_date 
	)
WHERE r = 2;



-- 3. From question 2 : when stat_cd is not euqal to state_cd then data issues else good data as stae_cd_status
--  [Note NUll from left side is not equal NUll from other side  >> means lets sayd NULL value from fact table if compared
--  to NULL Value to right table then it should be data issues]
SELECT dd.cust_id, tran_id, stat_cd, state_cd ,
	CASE 
		WHEN (dd.state_cd IS NULL) OR (tf.stat_cd IS NULL) THEN 'data issues'
		WHEN (dd.state_cd = tf.stat_cd) THEN 'good data'
		ELSE 'data issues'
	END
FROM cards_ingest.cust_dim_details dd
JOIN cards_ingest.tran_fact tf
ON dd.cust_id = tf.cust_id;