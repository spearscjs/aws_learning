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
    (102029, 'cust_104', 'TX', 1023,to_date('2022-01-05','yyy-mm-dd'));

1. Calculate total tran_ammt (sum) for each state
2. Calculate maximum and minimum tran_ammt on each state and tran_date
3. Calculate total transaction which have tran_ammt more than 10000
4. Show the state which have total (sum) tran_ammt more than 10000