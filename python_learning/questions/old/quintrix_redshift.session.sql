
/*
1. Create a schema lkp_data.
2. Create a table lkp_state_details. col (state_cd,population_cnt, potential_customer_cnt)
data:
NY,200,100
CA,500,200
TX,400,300
NV,100,90
NJ,200,70


Question:
1. Join  cards_ingest.tran_fact with lkp_state_details on state cd. Make sure if any Null Values from fact remove those records
Show me tran_date,state, number of customer per tran_date and state and number of customer company can target for promotion
who are not customer in but still lives in the state (population - number of customer)
    (changes per day)
    count of transaction table, then do minus for day
2. To reach each remaining potential_customer_cnt cost 5$, then show me the states where company has to spend 2nd high $ amount.
(make sure do potential_customer_cnt -allready customer count to get remaining potential customer count)

*/



CREATE SCHEMA IF NOT EXISTS lkp_data;

CREATE TABLE IF NOT EXISTS lkp_data.lkp_state_details(
    stat_cd varchar(2),
    population_cnt int,
    potential_customer_cnt int
    
);

TRUNCATE TABLE lkp_state_details;

INSERT INTO lkp_state_details(stat_cd, population_cnt, potential_customer_cnt)
    VALUES
        ('NY',200,100),
        ('CA',500,200),
        ('TX',400,300),
        ('NV',100,90),
        ('NJ',200,70)
    ;



/*
Question:
1. Join  cards_ingest.tran_fact with lkp_state_details on state cd. Make sure if any Null Values from fact remove those records
Show me tran_date,state, number of customer per tran_date and state and number of customer company can target for promotion
who are not customer in but still lives in the state (population - number of customer)
    (changes per day)
    count of transaction table, then do minus for day
*/

WITH 
-- users per day, change representation of date so doesnt include timestamp 
uniques AS (
    SELECT DISTINCT cust_id, stat_cd,EXTRACT(YEAR FROM tran_date) AS yr, EXTRACT(MONTH FROM tran_date) AS mth, EXTRACT(DAY FROM tran_date) AS dy,
    MIN(tran_date) OVER (PARTITION BY cust_id, stat_cd) cust_min_date
    FROM cards_ingest.tran_fact tf
    WHERE tf.* IS NOT NULL ORDER BY yr,mth,dy,stat_cd
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
-- sum of new cust per day
sum_new_cust_per_day AS (
    SELECT yr, mth, dy, stat_cd,
        SUM(new_cust_per_day) OVER (PARTITION BY stat_cd ORDER BY yr, mth, dy) AS total_customers_per_state
    FROM new_cust_per_day nc
),
-- number of customers per day per state
day_counts AS (
    SELECT yr,mth,dy,stat_cd, COUNT(*) AS cust_per_day FROM uniques GROUP BY yr,mth,dy,stat_cd 
)

SELECT DISTINCT dc.yr, dc.mth, dc.dy, dc.stat_cd, cust_per_day, 
    population_cnt, total_customers_per_state, 
    population_cnt - total_customers_per_state AS total_promotion_target
FROM day_counts dc 
FULL OUTER JOIN sum_new_cust_per_day snc
    ON  dc.stat_cd = snc.stat_cd AND dc.stat_cd = snc.stat_cd  AND dc.mth = snc.mth AND dc.yr = snc.yr AND dc.dy = snc.dy 
INNER JOIN lkp_data.lkp_state_details sd 
    ON sd.stat_cd = dc.stat_cd
ORDER BY yr, mth, dy, stat_cd;





/*

2. To reach each remaining potential_customer_cnt cost 5$, then show me the states where company has to spend 2nd high $ amount.
(make sure do potential_customer_cnt -allready customer count to get remaining potential customer count)

*/
WITH 
-- users per day, change representation of date so doesnt include timestamp 
uniques AS (
    SELECT DISTINCT cust_id, stat_cd,EXTRACT(YEAR FROM tran_date) AS yr, EXTRACT(MONTH FROM tran_date) AS mth, EXTRACT(DAY FROM tran_date) AS dy,
    MIN(tran_date) OVER (PARTITION BY cust_id, stat_cd) cust_min_date
    FROM cards_ingest.tran_fact tf
    WHERE tf.* IS NOT NULL ORDER BY yr,mth,dy,stat_cd
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
-- sum of new cust per day
sum_new_cust_per_day AS (
    SELECT yr, mth, dy, stat_cd,
        SUM(new_cust_per_day) OVER (PARTITION BY stat_cd ORDER BY yr, mth, dy) AS total_customers_per_state
    FROM new_cust_per_day nc
),
-- number of customers per day per state
day_counts AS (
    SELECT yr,mth,dy,stat_cd, COUNT(*) AS cust_per_day FROM uniques GROUP BY yr,mth,dy,stat_cd 
),
total_promotion AS ( 
    SELECT DISTINCT dc.yr, dc.mth, dc.dy, dc.stat_cd, cust_per_day, 
        potential_customer_cnt, total_customers_per_state, 
        (potential_customer_cnt - total_customers_per_state) * 5  AS total_promotion_cost
    FROM day_counts dc 
    FULL OUTER JOIN sum_new_cust_per_day snc
        ON  dc.stat_cd = snc.stat_cd AND dc.stat_cd = snc.stat_cd  AND dc.mth = snc.mth AND dc.yr = snc.yr AND dc.dy = snc.dy 
    INNER JOIN lkp_data.lkp_state_details sd 
        ON sd.stat_cd = dc.stat_cd
    ORDER BY yr, mth, dy, stat_cd
)

SELECT stat_cd, total_promotion_cost, rank FROM 
    (SELECT stat_cd, RANK() OVER (ORDER BY total_promotion_cost) rank, total_promotion_cost FROM 
        (SELECT DISTINCT stat_cd, MIN(total_promotion_cost) OVER (PARTITION BY stat_cd) total_promotion_cost
        FROM total_promotion) a) t WHERE rank=2;







/*



3. Same as question 1. But the number of customer from transaction table is total number of unique customer till that date .
(Hint use window function)
4. Same as question 2. If state cd is NULL  and cust_id is cust_109 then make sure to change to TX  else CA and calculate states where
company has to spend 2nd lowest $ amount from .
5. Show me the total number of customer company has , total population and potential_customer_cnt across all the states


*/
