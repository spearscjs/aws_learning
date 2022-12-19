--- Basic sql complexity is Easy. This needs to be solved first before going to question2

-- 1. Display all the information of the Employee table.
SELECT * FROM cards_ingest.emp;


-- 2. Display unique Department names from Employee table.
-- department numbers from employee table
SELECT DISTINCT deptno FROM cards_ingest.emp;

-- department names from employee table
SELECT DISTINCT dname 
FROM  cards_ingest.dept dept 
JOIN cards_ingest.emp emp ON emp.deptno = dept.deptno;


-- 3. List the details of the employees in ascending order of their salaries.
SELECT * FROM cards_ingest.emp ORDER BY sal ASC;


-- 4. List the employees who joined before 1981.
SELECT empno, ename FROM cards_ingest.emp WHERE EXTRACT(YEAR FROM hiredate) < 1981;


-- 5. List the employees who are joined in the year 1981
SELECT empno, ename FROM cards_ingest.emp WHERE EXTRACT(YEAR FROM hiredate) = 1981;


-- 6. List the Empno, Ename, Sal, Daily Sal of all Employees in the ASC order of AnnSal. (Note devide sal/30 as annsal)
SELECT empno, ename, sal, ROUND(sal/30,2) AS annsal FROM cards_ingest.emp ORDER BY annsal;


-- 7. List the employees who are working for the department name ACCOUNTING
SELECT empno, ename 
FROM cards_ingest.emp emp 
JOIN cards_ingest.dept dept ON emp.deptno = dept.deptno 
WHERE dname = 'ACCOUNTING';


-- 8. List the employees who does not belong to department name ACCOUNTING
SELECT empno, ename 
FROM cards_ingest.emp emp 
JOIN cards_ingest.dept dept ON emp.deptno = dept.deptno 
WHERE dname != 'ACCOUNTING';