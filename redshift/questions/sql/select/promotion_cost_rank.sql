/*

2. To reach each remaining potential_customer_cnt cost 5$, then show me the states where company has to spend 2nd high $ amount.
(make sure do potential_customer_cnt -allready customer count to get remaining potential customer count)

on this day if unique cust 1, can target, 200-x

day 1 -- 5 customer
    on that day -- total targer -  each day

4. Same as question 2. If state cd is NULL  and cust_id is cust_109 then make sure to change to TX  else CA and calculate states where
company has to spend 2nd lowest $ amount from .

*/

WITH rep_null AS (
    SELECT DISTINCT cust_id, tran_date,
        CASE 
            WHEN (stat_cd IS NULL) THEN
                CASE 
                    WHEN (cust_id = 'cust_109') THEN 'TX'
                    ELSE 'CA'
                END 
            ELSE stat_cd
        END AS stat_cd
    FROM cards_ingest.tran_fact tf
),
tf AS (
    SELECT DISTINCT cust_id, tf.stat_cd, tran_date, potential_customer_cnt
    FROM rep_null tf 
    INNER JOIN lkp_data.lkp_state_details sd
    ON tf.stat_cd = sd.stat_cd
),
promo AS (
    SELECT stat_cd, tran_date, COUNT(*) num_cust_per_date, (potential_customer_cnt - COUNT(*)) * 5 promotion_cost
    FROM tf
    GROUP BY tran_date, stat_cd, potential_customer_cnt
) 
SELECT stat_cd, tran_date, num_cust_per_date, promotion_cost FROM 
    (SELECT stat_cd, tran_date, num_cust_per_date, promotion_cost, RANK() OVER (PARTITION BY tran_date ORDER BY promotion_cost) r 
    FROM promo) pc
WHERE r = 2;
