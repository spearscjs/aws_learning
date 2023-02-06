
/*
    1. Create a schema lkp_data.
    2. Create a table lkp_state_details. col (state_cd,population_cnt, potential_customer_cnt)
    data:
    NY,200,100
    CA,500,200
    TX,400,300
    NV,100,90
    NJ,200,70
*/
BEGIN;
    CREATE SCHEMA IF NOT EXISTS lkp_data;
    DROP TABLE IF EXISTS lkp_data.lkp_state_details;
    CREATE TABLE IF NOT EXISTS lkp_data.lkp_state_details(
        stat_cd varchar(2),
        population_cnt int,
        potential_customer_cnt int
        
    );
    TRUNCATE TABLE lkp_data.lkp_state_details;
    INSERT INTO lkp_data.lkp_state_details
        (stat_cd, population_cnt, potential_customer_cnt)
        VALUES
            ('NY',200,100),
            ('CA',500,200),
            ('TX',400,300),
            ('NV',100,90),
            ('NJ',200,70);
    COMMIT;
END;


/*
TRUNCATE / REINSERT VALUES OF TRAN_FACT
*/
/*
BEGIN;
    CREATE SCHEMA IF NOT EXISTS cards_ingest;
    DROP TABLE IF EXISTS cards_ingest.tran_fact;
    create table cards_ingest.tran_fact(
        tran_id int,
        cust_id varchar(10),
        stat_cd varchar(2),
        tran_ammt decimal(10,2),
        tran_date date
    );
    TRUNCATE TABLE cards_ingest.tran_fact;
    INSERT INTO cards_ingest.tran_fact
        (tran_id, cust_id, stat_cd, tran_ammt, tran_date) 
        VALUES
            (102020, 'cust_101', 'NY', 125,to_date('2022-01-01','yyyy-mm-dd')),
            (102021, 'cust_101', 'NY', 5125,to_date('2022-01-01','yyyy-mm-dd')),
            (1020321, 'cust_101', 'NY', 225,to_date('2022-02-01','yyyy-mm-dd')),
            (1020121, 'cust_101', 'NY', 4125,to_date('2022-02-03','yyyy-mm-dd')),
            (1020222, 'cust_102', 'CA', 6125,to_date('2022-01-01','yyyy-mm-dd')),
            (1020223, 'cust_103', 'CA', 7145,to_date('2022-01-01','yyyy-mm-dd')),
            (1023023, 'cust_103', 'CA', 7145,to_date('2022-04-01','yyyy-mm-dd')),
            (1020123, 'cust_103', 'CA', 7145,to_date('2022-03-01','yyyy-mm-dd')),
            (1020223, 'cust_103', 'CA', 7145,to_date('2022-03-02','yyyy-mm-dd')),
            (102024, 'cust_104', 'TX', 1023,to_date('2022-01-01','yyyy-mm-dd')),
            (102025, 'cust_101', 'NY', 670,to_date('2022-01-03','yyyy-mm-dd')),
            (102026, 'cust_101', 'NY', 5235,to_date('2022-01-03','yyyy-mm-dd')),
            (102027, 'cust_102', 'CA', 61255,to_date('2022-01-04','yyyy-mm-dd')),
            (102028, 'cust_103', 'CA', 7345,to_date('2022-01-04','yyyy-mm-dd')),
            (102029, 'cust_104', 'TX', 1023,to_date('2022-01-05','yyyy-mm-dd')),
            (102030, 'cust_109', NULL, 1023,to_date('2022-01-05','yyyy-mm-dd')),
            (102031, 'cust_104',Null, 1023,to_date('2022-01-05','yyyy-mm-dd')),
            (102031, 'cust_107','TX', 4000,to_date('2022-01-05','yyyy-mm-dd')),
            (1022031, 'cust_107','TX', 4000,to_date('2022-02-05','yyyy-mm-dd')),
            (10202231, 'cust_107','TX', 4000,to_date('2022-02-03','yyyy-mm-dd')),
            (1302031, 'cust_107','CA', 7000,to_date('2022-02-05','yyyy-mm-dd')),
            (10202231, 'cust_111','NV', 10000,to_date('2022-02-03','yyyy-mm-dd')),
            (10202231, 'cust_111','NV', 9000,to_date('2022-07-03','yyyy-mm-dd'));
    COMMIT;
END;
*/


/*
Question:
1. Join  cards_ingest.tran_fact with lkp_state_details on state cd. Make sure if any Null Values from fact remove those records
Show me tran_date,state, number of customer per tran_date and state and number of customer company can target for promotion
who are not customer in but still lives in the state (population - number of customer)
    (changes per day)
    count of transaction table, then do minus for day
*/

-- ASSUMING EACH UNIQUE CUSTOMER DOES NOT AFFECT THE POPULATION TOTAL FOR THE NEXT DAY
WITH tf AS (
    SELECT DISTINCT cust_id, tf.stat_cd, tran_date, population_cnt
    FROM cards_ingest.tran_fact tf 
    INNER JOIN lkp_data.lkp_state_details sd
    ON tf.stat_cd = sd.stat_cd
    WHERE tran_id IS NOT NULL AND cust_id IS NOT NULL AND tf.stat_cd IS NOT NULL 
        AND tran_ammt IS NOT NULL AND tran_date IS NOT NULL
)
SELECT stat_cd, tran_date, COUNT(*) num_cust_per_date, population_cnt - COUNT(*) num_promotion_cust
FROM tf
GROUP BY tran_date, stat_cd, population_cnt;



-- ASSUMING EACH UNIQUE CUSTOMER AFFECT THE POPULATION TOTAL FOR THE NEXT DAY
-- total unique customers by state,day
WITH total_unique AS ( 
    SELECT DISTINCT tran_date, stat_cd,
        (SELECT COUNT(DISTINCT cust_id) FROM cards_ingest.tran_fact WHERE tran_date <= tf.tran_date AND stat_cd = tf.stat_cd) total_customers_per_state,
        (SELECT COUNT(DISTINCT cust_id) cust_per_day FROM cards_ingest.tran_fact WHERE stat_cd = tf.stat_cd AND tran_date = tf.tran_date) daily_cust_count
    FROM cards_ingest.tran_fact tf
    WHERE
        -- dont allow null values in any column
        tran_id IS NOT NULL AND cust_id IS NOT NULL AND tf.stat_cd IS NOT NULL 
            AND tran_ammt IS NOT NULL AND tran_date IS NOT NULL
    ORDER BY stat_cd,tran_date
)
SELECT DISTINCT tu.*, 
    population_cnt,
    population_cnt - total_customers_per_state AS total_promotion_target
FROM total_unique tu
INNER JOIN lkp_data.lkp_state_details sd 
    ON sd.stat_cd = tu.stat_cd
ORDER BY tran_date, stat_cd;





/*

2. To reach each remaining potential_customer_cnt cost 5$, then show me the states where company has to spend 2nd high $ amount.
(make sure do potential_customer_cnt -allready customer count to get remaining potential customer count)

on this day if unique cust 1, can target, 200-x

day 1 -- 5 customer
    on that day -- total targer -  each day

*/
-- ASSUMING EACH UNIQUE CUSTOMER DOES NOT AFFECT THE POPULATION TOTAL FOR THE NEXT DAY
WITH tf AS (
    SELECT DISTINCT cust_id, tf.stat_cd, tran_date, potential_customer_cnt
    FROM cards_ingest.tran_fact tf 
    INNER JOIN lkp_data.lkp_state_details sd
    ON tf.stat_cd = sd.stat_cd
    WHERE tran_id IS NOT NULL AND cust_id IS NOT NULL AND tf.stat_cd IS NOT NULL 
        AND tran_ammt IS NOT NULL AND tran_date IS NOT NULL 
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



-- ASSUMING EACH UNIQUE CUSTOMER AFFECT THE POPULATION TOTAL FOR THE NEXT DAY
WITH total_unique AS ( 
    SELECT DISTINCT tran_date, stat_cd,
        (SELECT COUNT(DISTINCT cust_id) FROM cards_ingest.tran_fact WHERE tran_date <= tf.tran_date AND stat_cd = tf.stat_cd) total_customers_per_state,
        (SELECT COUNT(DISTINCT cust_id) cust_per_day FROM cards_ingest.tran_fact WHERE stat_cd = tf.stat_cd AND tran_date = tf.tran_date) daily_cust_count
    FROM cards_ingest.tran_fact tf
    WHERE
        -- dont allow null values in any column
        tran_id IS NOT NULL AND cust_id IS NOT NULL AND tf.stat_cd IS NOT NULL 
            AND tran_ammt IS NOT NULL AND tran_date IS NOT NULL
    ORDER BY stat_cd,tran_date
),
total_promotion AS ( 
    SELECT DISTINCT tu.tran_date, tu.stat_cd, 
        potential_customer_cnt,
        potential_customer_cnt - total_customers_per_state AS total_promotion_target,
        (potential_customer_cnt - total_customers_per_state) * 5 AS total_promotion_cost
    FROM total_unique tu
    INNER JOIN lkp_data.lkp_state_details sd 
        ON sd.stat_cd = tu.stat_cd
    ORDER BY tran_date, stat_cd
)
SELECT stat_cd, total_promotion_cost, rank FROM 
    (SELECT stat_cd, RANK() OVER (ORDER BY total_promotion_cost) rank, total_promotion_cost FROM 
        (SELECT DISTINCT stat_cd, MIN(total_promotion_cost) OVER (PARTITION BY stat_cd) total_promotion_cost
        FROM total_promotion) a) t WHERE rank=2;




/*
3. Same as question 1. But the number of customer from transaction table is total number of unique customer till that date .
(Hint use window function)
*/
SELECT DISTINCT tran_date, stat_cd,
        (SELECT COUNT(DISTINCT cust_id) FROM cards_ingest.tran_fact WHERE tran_date <= tf.tran_date AND stat_cd = tf.stat_cd) total_customers_per_state,
        (SELECT COUNT(DISTINCT cust_id) cust_per_day FROM cards_ingest.tran_fact WHERE stat_cd = tf.stat_cd AND tran_date = tf.tran_date) daily_cust_count
    FROM cards_ingest.tran_fact tf
    WHERE
        -- dont allow null values in any column
        tran_id IS NOT NULL AND cust_id IS NOT NULL AND tf.stat_cd IS NOT NULL 
            AND tran_ammt IS NOT NULL AND tran_date IS NOT NULL
    ORDER BY stat_cd,tran_date

/*
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



/*
5. Show me the total number of customer company has , total population and potential_customer_cnt across all the states
*/
WITH total_unique AS ( 
    SELECT DISTINCT tran_date, stat_cd,
        (SELECT COUNT(DISTINCT cust_id) FROM cards_ingest.tran_fact WHERE tran_date <= tf.tran_date AND stat_cd = tf.stat_cd) total_customers_per_state,
        (SELECT COUNT(DISTINCT cust_id) cust_per_day FROM cards_ingest.tran_fact WHERE stat_cd = tf.stat_cd AND tran_date = tf.tran_date) daily_cust_count
    FROM cards_ingest.tran_fact tf
    WHERE
        -- dont allow null values in any column
        tran_id IS NOT NULL AND cust_id IS NOT NULL AND tf.stat_cd IS NOT NULL 
            AND tran_ammt IS NOT NULL AND tran_date IS NOT NULL
    ORDER BY stat_cd,tran_date
)
SELECT tu.stat_cd, MAX(total_customers_per_state) total_customers, population_cnt, potential_customer_cnt
FROM total_unique tu INNER JOIN lkp_data.lkp_state_details sd 
    ON sd.stat_cd = tu.stat_cd
GROUP BY tu.stat_cd, population_cnt, potential_customer_cnt 