/*
Question:
1. Join  cards_ingest.tran_fact with lkp_state_details on state cd. Make sure if any Null Values from fact remove those records
Show me tran_date,state, number of customer per tran_date and state and number of customer company can target for promotion
who are not customer in but still lives in the state (population - number of customer)
    (changes per day)
    count of transaction table, then do minus for day
*/
/*
3. Same as question 1. But the number of customer from transaction table is total number of unique customer till that date .
(Hint use window function)
*/

WITH 
-- users per day, change representation of date so doesnt include timestamp 
uniques AS (
    SELECT DISTINCT cust_id, stat_cd,EXTRACT(YEAR FROM tran_date) AS yr, EXTRACT(MONTH FROM tran_date) AS mth, EXTRACT(DAY FROM tran_date) AS dy,
    MIN(tran_date) OVER (PARTITION BY cust_id, stat_cd) cust_min_date
    FROM cards_ingest.tran_fact tf
    WHERE tran_id IS NOT NULL AND cust_id IS NOT NULL AND tf.stat_cd IS NOT NULL 
        AND tran_ammt IS NOT NULL AND tran_date IS NOT NULL 
    ORDER BY yr,mth,dy,stat_cd
),
-- number of new customers per day per state
new_cust_per_day AS ( 
    SELECT DISTINCT u.stat_cd, u.yr, u.mth, u.dy, COALESCE(new_cust_per_day, 0) AS new_cust_per_day 
    FROM uniques u LEFT JOIN 
        (  
            SELECT DISTINCT yr,mth,dy,u.stat_cd,  
            COUNT(*) OVER (PARTITION BY u.stat_cd, yr,mth,dy) AS new_cust_per_day
                FROM uniques u 
                WHERE mth = EXTRACT(MONTH FROM cust_min_date) AND yr = EXTRACT(YEAR FROM cust_min_date) AND dy = EXTRACT(DAY FROM cust_min_date) 
                GROUP BY yr,mth,dy,u.stat_cd,cust_id
        ) nc
    ON 
        nc.yr = u.yr AND nc.mth = u.mth AND nc.dy = u.dy  AND nc.stat_cd = u.stat_cd
),
sum_new_cust_per_day AS (
    SELECT yr, mth, dy, stat_cd, new_cust_per_day,
        SUM(new_cust_per_day) OVER (PARTITION BY stat_cd ORDER BY yr, mth, dy 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS total_customers_per_state
    FROM new_cust_per_day nc
)
SELECT * FROM sum_new_cust_per_day;