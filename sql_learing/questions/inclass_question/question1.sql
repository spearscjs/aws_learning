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
	(102020, 'cust_101', 'NY', 125,to_date('2022-01-01','yyy-mm-dd')),
	(102021, 'cust_101', 'NY', 5125,to_date('2022-01-01','yyy-mm-dd')),
    (102022, 'cust_102', 'CA', 6125,to_date('2022-01-01','yyy-mm-dd')),
    (102023, 'cust_103', 'CA', 7145,to_date('2022-01-01','yyy-mm-dd')),
    (102024, 'cust_104', 'TX', 1023,to_date('2022-01-01','yyy-mm-dd')),
    (102025, 'cust_101', 'NY', 670,to_date('2022-01-03','yyy-mm-dd')),
	(102026, 'cust_101', 'NY', 5235,to_date('2022-01-03','yyy-mm-dd')),
    (102027, 'cust_102', 'CA', 61255,to_date('2022-01-04','yyy-mm-dd')),
    (102028, 'cust_103', 'CA', 7345,to_date('2022-01-04','yyy-mm-dd')),
    (102029, 'cust_104', 'TX', 1023,to_date('2022-01-05','yyy-mm-dd')),
    (102030, 'cust_109', NULL, 1023,to_date('2022-01-05','yyy-mm-dd')),
    (102031, 'cust_104',Null, 1023,to_date('2022-01-05','yyy-mm-dd'),
    (102031, 'cust_107',TX, 1023,to_date('2022-01-05','yyy-mm-dd'),
    (102031, 'cust_107',CA, 1023,to_date('2022-02-05','yyy-mm-dd'));


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
('cust_101','NY','08922', 'Mike', 'doge',to_date('2022-01-01','yyy-mm-dd'),to_date('2029-01-01','yyy-mm-dd'),'Y'),
('cust_102','CA','04922', 'sean', 'lan',to_date('2022-01-01','yyy-mm-dd'),to_date('2029-01-01','yyy-mm-dd'),'Y'),
('cust_103','CA','05922', 'sachin', 'ram',to_date('2022-01-01','yyy-mm-dd'),to_date('2029-01-01','yyy-mm-dd'),'Y'),
('cust_104','TX','08922', 'bill', 'kja',to_date('2022-01-01','yyy-mm-dd'),to_date('2029-01-01','yyy-mm-dd'),'Y'),
('cust_105','CA','08922', 'Douge', 'lilly',to_date('2022-01-01','yyy-mm-dd'),to_date('2029-01-01','yyy-mm-dd'),'Y'),
('cust_106','CA','08922', 'hence', 'crow',to_date('2022-01-01','yyy-mm-dd'),to_date('2029-01-01','yyy-mm-dd'),'Y'),
('cust_107','TX','08922', 'Mike', 'doge',to_date('2022-01-01','yyy-mm-dd'),to_date('2029-02-01','yyy-mm-dd'),'Y'),
('cust_107','NY','08922', 'Mike', 'doge',to_date('2022-02-03','yyy-mm-dd'),to_date('2022-01-01','yyy-mm-dd'),'N');


1. Calculate total tran_ammt (sum) for each state
2. Calculate maximum and minimum tran_ammt on each state and tran_date
3. Calculate total transaction which have tran_ammt more than 10000
4. Show the state which have total (sum) tran_ammt more than 10000
5. show me the states where total ammt is more than 10000
6. show me the states where cust_id ='cust_104' and  total ammt is more than 10000
7. Calculate total transaction by state [ if state if NULL make it TX] where total transaction is more than 10000
8. Show me a message col if state is null then "missing data" else "good data"
9. Show me sum of tran_ammt by state [ if state is null and cust_id='cust_104' then 'TX' else 'CA']




Join Question:

1.Give me all details from transaction tale and zip_cd from dimension table.
2. Sum of tran_ammt by zip_cd
