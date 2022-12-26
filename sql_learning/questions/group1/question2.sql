-- 1.SQL Query to print the number of employees per department in the organization
SELECT deptno, COUNT(*) AS total_employees 
FROM cards_ingest.emp 
GROUP BY deptno;



-- 2.SQL Query to find the employee details who got second maximum salary
SELECT empno, ename, sal 
FROM (
		SELECT empno, ename, sal, DENSE_RANK() OVER(ORDER BY sal DESC) r 
		FROM cards_ingest.emp
	) e
WHERE r = 2;



-- 3.SQL Query to find the employee details who got second maximum salary in each department
SELECT empno, ename, sal, deptno 
FROM
( 
	SELECT empno, ename, sal, deptno, DENSE_RANK() OVER(PARTITION BY deptno ORDER BY sal DESC) r 
	FROM cards_ingest.emp
) e
WHERE r = 2;



-- 4.SQL Query to find the employee who got minimum salary in 2019
SELECT empno, ename FROM cards_ingest.emp 
WHERE emp.sal IN (SELECT MIN(sal) FROM cards_ingest.emp);



-- 5.SQL query to select the employees getting salary greater than the average salary of the department that are working in
SELECT * FROM cards_ingest.emp emp 
JOIN (SELECT deptno, AVG(sal) avg_sal FROM cards_ingest.emp GROUP BY deptno) dept_avg
ON emp.deptno = dept_avg.deptno AND emp.sal > dept_avg.avg_sal;



-- 6.SQL query to compute the group salary of all the employees .
SELECT SUM(sal) AS group_salary FROM cards_ingest.emp;


-- 7.SQL query to list the employees and name of employees reporting to each person.
SELECT emp.empno, emp.ename, mgr.ename AS mgr_name
FROM cards_ingest.emp emp JOIN cards_ingest.emp mgr
ON emp.mgr = mgr.empno;



-- 8.SQL query to find the department with highest number of employees.
-- (query ensures if >1 departments are tied for first, they will all show )
SELECT deptno, COUNT(*) num_employees FROM cards_ingest.emp emp 
GROUP BY deptno
HAVING COUNT(*) IN
	(SELECT MAX(num_emp) FROM 
		(SELECT COUNT(*) AS num_emp FROM cards_ingest.emp GROUP BY deptno) max_employees);















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

drop table cards_ingest.cust_dim_details;
create table cards_ingest.cust_dim_details (
    cust_id varchar(10),
    state_cd varchar(2),
    zip_cd varchar(5),
    cust_first_name  varchar(20),
    cust_last_name  varchar(20),
    start_date date,
    end_date date,
    active_flag varchar(1)
);
truncate table cards_ingest.cust_dim_details;

insert into cards_ingest.cust_dim_details
(cust_id,state_cd,zip_cd , cust_first_name, cust_last_name, start_date,end_date,active_flag)
VALUES
('cust_101','NY','08922', 'Mike', 'doge',to_date('2022-01-01','yyyy-mm-dd'),to_date('2029-01-01','yyyy-mm-dd'),'Y'),
('cust_102','CA','04922', 'sean', 'lan',to_date('2022-01-01','yyyy-mm-dd'),to_date('2029-01-01','yyyy-mm-dd'),'Y'),
('cust_103','CA','05922', 'sachin', 'ram',to_date('2022-01-01','yyyy-mm-dd'),to_date('2029-01-01','yyyy-mm-dd'),'Y'),
('cust_104','TX','08942', 'bill', 'kja',to_date('2022-01-01','yyyy-mm-dd'),to_date('2029-01-01','yyyy-mm-dd'),'Y'),
('cust_105','CA','08122', 'Douge', 'lilly',to_date('2022-01-01','yyyy-mm-dd'),to_date('2029-01-01','yyyy-mm-dd'),'Y'),
('cust_106','CA','08322', 'hence', 'crow',to_date('2022-01-01','yyyy-mm-dd'),to_date('2029-01-01','yyyy-mm-dd'),'Y'),
('cust_107','TX','08722', 'Mike', 'dogeee',to_date('2022-01-01','yyyy-mm-dd'),to_date('2022-02-01','yyyy-mm-dd'),'Y'),
('cust_107','TX','08723', 'Mike', Null,to_date('2022-02-02','yyyy-mm-dd'),to_date('2029-09-01','yyyy-mm-dd'),'Y'),
('cust_107','NY','02122', 'Mike', 'doge',to_date('2022-02-05','yyyy-mm-dd'),to_date('2022-02-09','yyyy-mm-dd'),'N'),
('cust_111','NV','09812', 'Hary', 'roel',to_date('2022-02-10','yyyy-mm-dd'),to_date('2029-01-01','yyyy-mm-dd'),'N');




/*


Answer Query:

1.with all_cust as (
select  sum(tran_ammt) tot_ammount ,tran_date,cust_id from cards_ingest.tran_fact
group by 2,3
),
get_prev as ( select cust_id,tran_date,tot_ammount,
                     nvl(lag(tot_ammount,1) over(partition by cust_id order by tran_date),0) as prev_tot_ammount,
                     coalesce(lag(tran_date,1) over(partition by cust_id order by tran_date),'2022-01-01') as prev_tran_date
              from all_cust )
select * from get_prev where prev_tot_ammount > tot_ammount

*/







-- 5.  Show me the month wise transaction ammount and zip_cd
SELECT EXTRACT(MONTH from tran_date) AS month, EXTRACT(YEAR from tran_date) AS year, zip_cd, SUM(tran_ammt) AS total_tran 
FROM cards_ingest.tran_fact tf
    JOIN cards_ingest.cust_dim_details cdd
    ON tf.cust_id = cdd.cust_id
GROUP BY zip_cd, month, year;

-- 6. Show me active customer per month?
SELECT cust_id, EXTRACT(MONTH from start_date) AS month, EXTRACT(YEAR from start_date) AS year 
FROM cards_ingest.cust_dim_details WHERE active_flag = 'Y';


-- 7. delete records which are duplicate from cust_dim_details  table. (Keep the latest record only by date)
DELETE FROM cards_ingest.cust_dim_details cdd
    WHERE  cdd.start_date < (SELECT max(start_date) FROM cards_ingest.cust_dim_details cdd2 
    WHERE cdd2.cust_id = cdd.cust_id);



-- 8. Show me all the custid and tranid and total transaction by cust id where you don't have matching record in cust_dim_details
-- [join on cust id and check trandt between start_date,end_date]

-- USE OUTER JOIN



SELECT tf.cust_id, tran_id, SUM(tran_ammt) OVER (PARTITION BY tf.cust_id ORDER BY tf.cust_id, tran_date 
ROWS BETWEEN UNBOUNDED PRECEDING  AND CURRENT ROW) total_tran_ammt 
FROM cards_ingest.cust_dim_details cdd 
JOIN cards_ingest.tran_fact tf 
ON cdd.cust_id = tf.cust_id




SELECT tf.cust_id, tran_id, SUM(tran_ammt) OVER (PARTITION BY tf.cust_id ORDER BY tf.cust_id, tran_date 
ROWS BETWEEN UNBOUNDED PRECEDING  AND CURRENT ROW) total_tran_ammt 
FROM cards_ingest.cust_dim_details cdd 
JOIN cards_ingest.tran_fact tf 
ON cdd.cust_id = tf.cust_id

ORDER BY cust_id, state_cd, tran_date, tran_ammt 




SELECT tf.cust_id, tran_id, COUNT(1) OVER (PARTITION BY tf.cust_id ORDER BY tf.cust_id, tran_date 
ROWS BETWEEN UNBOUNDED PRECEDING  AND CURRENT ROW) total_tran_ammt 
FROM cards_ingest.cust_dim_details cdd 
JOIN cards_ingest.tran_fact tf 
ON cdd.cust_id = tf.cust_id



-- this
SELECT tf.cust_id, tran_id, SUM(tran_ammt) OVER (PARTITION BY tf.cust_id ORDER BY tf.cust_id, tran_date, tran_ammt) total_tran_ammt 
FROM cards_ingest.cust_dim_details cdd 
JOIN cards_ingest.tran_fact tf 
ON cdd.cust_id = tf.cust_id

-- vs 

-- this
SELECT tf.cust_id, tran_id, SUM(tran_ammt) OVER (PARTITION BY tf.cust_id ORDER BY tf.cust_id, tran_date) total_tran_ammt 
FROM cards_ingest.cust_dim_details cdd 
JOIN cards_ingest.tran_fact tf 
ON cdd.cust_id = tf.cust_id










/*
what kind of join? 
    out or inner? and why?
    review video


outer join -- all record mathcing and not matching
    -- cust id + tran id between
    -- if not still get


-- what if both outer join are null?

*?




/*


select cust_id,stat_cd,tran_date,tran_ammt ,
       sum(tran_ammt) over(partition by cust_id order by tran_date
       ROWS between UNBOUNDED PRECEDING and CURRENT ROW)  as running_tran_ammt
from cards_ingest.tran_fact where cust_id='cust_101'
order by cust_id,stat_cd,tran_date

 
select cust_id,stat_cd,tran_date,tran_ammt from cards_ingest.tran_fact  where cust_id='cust_101'
order by cust_id,stat_cd,tran_date,tran_ammt
 
d0,e1,1,1
d1,e2,2,3
d2,e1,3,6
d3,e2,5,11
 
d0,e1,1
d1,e2,2
d2,e1,4
d3,e2,7
 
select cust_id,stat_cd,tran_date,tran_ammt ,
       sum(tran_ammt) over( order by tran_date
       ROWS between UNBOUNDED PRECEDING and CURRENT ROW)  as running_tran_ammt
from cards_ingest.tran_fact --where cust_id='cust_101'
order by cust_id,stat_cd,tran_date

 
-- Show all the cust id,state cd, total number of trasaction (Include all the transactio per state cd) and
-- total_transaction ammont per state cd, total number of trasaction (don't include any transaction which are less than 1000)

 
select cust_id,stat_cd,tran_date ,tran_ammt,
       sum(1) over( partition by stat_cd )  as tran_count,
       sum(case when tran_ammt <1000 then 0 else 1 end ) over( partition by stat_cd )  as tran_count
from cards_ingest.tran_fact --where cust_id='cust_101'
order by cust_id,stat_cd,tran_date
 
select count(1),cust_id from cards_ingest.cust_dim_details
group by cust_id
having count(1) >1
 
-- 8. Show me all the custid and tranid and total transaction by cust id where you don't have matching record in cust_dim_details
-- [join on cust id and check trandt between start_date,end_date]
 
select fct.cust_id,tran_id,zip_cd,
sum(tran_ammt) over(partition by fct.cust_id) as tot_tran_ammt from cards_ingest.tran_fact fct
left outer join cards_ingest.cust_dim_details dm
on fct.cust_id=dm.cust_id and tran_date between start_date and end_date
where zip_cd is null



*/










