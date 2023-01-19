/*
1.Create table :tran_fact
tran_id int, cust_id varchar(20),tran_date date,tran_ammount decimal(10,2), tran_type varchar(1)

102020,CA1001,2022-02-01,1200,C
102021,CA1002,2022-02-01,700,C
102022,CA1003,2022-02-01,500,C
102023,CA1004,2022-02-02,900,C
102020,CA1001,2022-02-02,200,D
102029,CA1001,2022-02-02,700,C
102024,CA1005,2022-02-03,12200,C
102025,CA1003,2022-02-03,200,D
102026,CA1004,2022-02-04,12200,C
102027,CA1007,2022-02-04,9200,C
102028,CA1007,2022-02-04,3200,D
*/

CREATE SCHEMA IF NOT EXISTS hadoop_ingest;
DROP TABLE hadoop_ingest.tran_fact;
CREATE TABLE hadoop_ingest.tran_fact(  
    tran_id int, 
    cust_id varchar(20),
    tran_date date,
    tran_ammount decimal(10,2), 
    tran_type varchar(1)
);
INSERT INTO hadoop_ingest.tran_fact (tran_id, cust_id, tran_date, tran_ammount, tran_type) VALUES 
    (102021,'CA1002','2022-02-01',700,'C'),
    (102022,'CA1003','2022-02-01',500,'C'),
    (102020,'CA1001','2022-02-02',200,'C'),
    (102023,'CA1004','2022-02-02',900,'C'),
    (102020,'CA1001','2022-02-02',200,'D'),
    (102029,'CA1001','2022-02-02',700,'C'),
    (102024,'CA1005','2022-02-03',12200,'C'),
    (102025,'CA1003','2022-02-03',200,'D'),
    (102026,'CA1004','2022-02-04',12200,'C'),
    (102027,'CA1007','2022-02-04',9200,'C'),
    (102028,'CA1007','2022-02-04',3200,'D'),
    (102029,'CA1007','2022-02-05',3200,'D')
;

-- 1. Total unique customer per day.
SELECT tran_date, COUNT(cust_id) unique_customers FROM 
    (SELECT DISTINCT cust_id, tran_date FROM hadoop_ingest.tran_fact GROUP BY cust_id, tran_date) t
GROUP BY tran_date;

SELECT tran_date, COUNT(DISTINCT cust_id) unique_customers 
FROM hadoop_ingest.tran_fact GROUP BY tran_date;

-- 2. Total number of unique customer till date
/*
rank -- partition by cust order date


*/
WITH 
uniques AS (
    SELECT DISTINCT cust_id, tran_date, MIN(tran_date) OVER (PARTITION BY cust_id) cust_min_date 
    FROM hadoop_ingest.tran_fact GROUP BY cust_id, tran_date
),
new_cust_per_day AS ( 
    SELECT DISTINCT u.tran_date, COALESCE(new_cust_per_day, 0) AS new_cust_per_day 
    FROM uniques u LEFT JOIN 
        ( 
            SELECT DISTINCT tran_date,
                    COUNT(*) OVER (PARTITION BY tran_date) AS new_cust_per_day
                        FROM uniques u 
                        WHERE u.tran_date = u.cust_min_date
                        GROUP BY tran_date,cust_id
        ) nc
    ON nc.tran_date = u.tran_date
),
sum_new_cust_per_day AS (
    SELECT tran_date,
        SUM(new_cust_per_day) OVER (ORDER BY tran_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS total_customers
    FROM new_cust_per_day nc
)
SELECT * FROM sum_new_cust_per_day;



WITH ranks AS (
    SELECT DISTINCT cust_id, tran_date, RANK() OVER(PARTITION BY cust_id ORDER BY tran_date)
    FROM hadoop_ingest.tran_fact
)
SELECT DISTINCT tran_date, SUM(rank) OVER(ORDER BY tran_date) FROM ranks WHERE rank = 1;
-- lead or lag function with max????
-- can do sum, not coalesce but do csae statement, if then 0

;

part cust id order by date

-- 3. Total transaction amount per customer per day ( if its C then add if D then subtract )
SELECT tran_date, cust_id, SUM(tran_ammt) total_tran_ammt
FROM
    (SELECT tran_date, cust_id, 
        CASE 
            WHEN tran_type = 'C'
                THEN tran_ammount
            ELSE -tran_ammount
        END AS tran_ammt
    FROM hadoop_ingest.tran_fact) t
GROUP BY tran_date, cust_id



-- 4. Find out duplicate transaction in total.
SELECT tran_id, cust_id,  tran_date, tran_ammount, tran_type
FROM hadoop_ingest.tran_fact tf
GROUP BY tran_id, cust_id,  tran_date, tran_ammount, tran_type
HAVING COUNT(*) > 1;



-- 5. show the transaction which has debit but never credit before. 
WITH first_credit_date AS (
    SELECT DISTINCT tran_id, MIN(tran_date) OVER (PARTITION BY tran_id) first_credit_date
    FROM hadoop_ingest.tran_fact tf 
    WHERE tran_type = 'C'
)
SELECT  tf.tran_id, tf.cust_id, tf.tran_date, tf.tran_ammount, tf.tran_type, first_credit_date
FROM hadoop_ingest.tran_fact tf 
LEFT JOIN first_credit_date fcd
ON tf.tran_id = fcd.tran_id
WHERE tran_type = 'D' AND first_credit_date IS NULL;