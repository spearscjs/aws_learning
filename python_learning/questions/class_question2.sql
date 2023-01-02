
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
    - take potential, minus already, times by 5, (on that day, not total)
*/


create table cards_ingest.tran_fact(
    tran_id int,
    cust_id varchar(10),
    stat_cd varchar(2),
    tran_ammt decimal(10,2),
    tran_date date
)
truncate table cards_ingest.tran_fact;
INSERT INTO cards_ingest.tran_fact
	(tran_id, cust_id, stat_cd, tran_ammt, tran_date) VALUES
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



CREATE SCHEMA IF NOT EXISTS lkp_data;

DROP TABLE IF EXISTS lkp_data.lkp_state_details;

CREATE TABLE IF NOT EXISTS lkp_data.lkp_state_details(
    stat_cd varchar(2),
    population_cnt int,
    potential_customer_cnt int
    
);

TRUNCATE TABLE lkp_state_details;

INSERT INTO lkp_data.lkp_state_details(stat_cd, population_cnt, potential_customer_cnt)
    VALUES
        ('NY',200,100),
        ('CA',500,200),
        ('TX',400,300),
        ('NV',100,90),
        ('NJ',200,70)
    ;



/*
Question:

-- sql then python script 

1. Join  cards_ingest.tran_fact with lkp_state_details on state cd. Make sure if any Null Values from fact remove those records
Show me tran_date,state, number of customer per tran_date and state and number of customer company can target for promotion
who are not customer in but still lives in the state (population - number of customer)
    (changes per day)
    count of transaction table, then do minus for day
*/

SELECT * FROM lkp_data.lkp_state_details;
SELECT * FROM cards_ingest.tran_fact;

SELECT * FROM cards_ingest.tran_fact AS tf 
    FULL OUTER JOIN lkp_data.lkp_state_details AS lkp
    ON tf.stat_cd = lkp.stat_cd
    WHERE ;


