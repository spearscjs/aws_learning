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
    (1302031, 'cust_107','CA', 7000,to_date('2022-02-05','yyyy-mm-dd'));

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
('cust_107','TX','08722', 'Mike', Null,to_date('2022-02-02','yyyy-mm-dd'),to_date('2029-09-01','yyyy-mm-dd'),'Y'),
('cust_107','NY','02122', 'Mike', 'doge',to_date('2022-09-02','yyyy-mm-dd'),to_date('2029-01-01','yyyy-mm-dd'),'N');


question:
1. Find out all the cust id where the previous (last only) transaction is more then current transaction ammount?
2. Show the cust id, state cd, total trsaction ammount till date (running total) for all the records.
3. Show all the cust id,state cd, total number of trasaction (Include all the transactio per state cd) and
total_transaction ammont per state cd, total number of trasaction (don't include any transaction which are less than 1000)

Note : Here in case 1 you are including all the transaction to show the count but later you have tu just include only where >1000
4. in cust_dim_details change all the name from Mike to Nike where cust_last_name !='dogeee'