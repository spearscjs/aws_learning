create schema ORG;
USE ORG;

CREATE TABLE  org.Worker (
	WORKER_ID INT ,
	FIRST_NAME CHAR(25),
	LAST_NAME CHAR(25),
	SALARY INT,
	JOINING_DATE date,
	DEPARTMENT CHAR(25)
);

INSERT INTO org.Worker
	(WORKER_ID, FIRST_NAME, LAST_NAME, SALARY, JOINING_DATE, DEPARTMENT) VALUES
		(001, 'Monika', 'Arora', 100000, to_date('3-12-1981','dd-mm-yyyy'), 'HR'),
		(002, 'Niharika', 'Verma', 80000,to_date('3-12-1982','dd-mm-yyyy'), 'Admin'),
		(003, 'Vishal', 'Singhal', 300000, to_date('5-1-1980','dd-mm-yyyy'), 'HR'),
		(004, 'Amitabh', 'Singh', 500000, to_date('3-12-1980','dd-mm-yyyy'), 'Admin'),
		(005, 'Vivek', 'Bhati', 500000, to_date('3-12-1981','dd-mm-yyyy'), 'Admin'),
		(006, 'Vipul', 'Diwan', 200000, to_date('5-10-1981','dd-mm-yyyy'), 'Account'),
		(007, 'Satish', 'Kumar', 75000, to_date('1-1-1981','dd-mm-yyyy'), 'Account'),
		(008, 'Geetika', 'Chauhan', 90000, to_date('3-7-1986','dd-mm-yyyy'), 'Admin');