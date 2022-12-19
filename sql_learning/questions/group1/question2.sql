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
