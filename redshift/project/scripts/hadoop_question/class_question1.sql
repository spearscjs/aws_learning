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
    (102028,'CA1007','2022-02-05',3200,'C'),
    (102029,'CA1007','2022-02-05',3200,'D')
;

-- 1. Total unique customer per day.
SELECT tran_date, COUNT(DISTINCT cust_id) unique_customers 
FROM hadoop_ingest.tran_fact 
GROUP BY tran_date;

-- 2. Total number of unique customer till date
/*
rank -- partition by cust order date


*/
-- using sum
WITH ranks AS (
    SELECT DISTINCT cust_id, tran_date, RANK() OVER(PARTITION BY cust_id ORDER BY tran_date)
    FROM hadoop_ingest.tran_fact
)
SELECT DISTINCT tran_date, 
    SUM(CASE WHEN rank = 1 THEN 1 ELSE 0 END) OVER(ORDER BY tran_date) total_unique_cust 
FROM ranks
ORDER BY tran_date;


-- using nested select
SELECT DISTINCT tran_date,
       (SELECT COUNT(DISTINCT cust_id) FROM hadoop_ingest.tran_fact  WHERE tran_date <= t.tran_date) total_unique_cust
FROM hadoop_ingest.tran_fact t
GROUP BY tran_date
ORDER BY tran_date;



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
WITH counts AS(
    SELECT tran_id, tran_type, tran_date,
    -- count all 'C' type transactions with same id that happened before tran_date (if it is 0, there were no credits before the transaction) 
    (
        SELECT COUNT(tran_id) FROM hadoop_ingest.tran_fact 
        WHERE tran_date <= tf.tran_date AND tran_id = tf.tran_id AND tran_type = 'C'
    ) FROM hadoop_ingest.tran_fact tf)
SELECT tran_id, tran_date FROM counts WHERE tran_type = 'D' AND count = 0;