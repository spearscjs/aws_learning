#rm metastore_db/*.lck 

# create tran_fact
#hive -e 'drop table if exists src_customer.tran_fact'
hive -e 'drop table if exists src_customer.tran_fact'
hive -f sql/src_customer/cd_src_customer.hql
hive -f sql/src_customer/ct_tran_fact.hql

# partition table
hive -e "
alter table src_customer.tran_fact add partition (dataset_date='2023-01-01');
alter table src_customer.tran_fact add partition (dataset_date='2023-01-02');
alter table src_customer.tran_fact add partition (dataset_date='2023-01-03');
alter table src_customer.tran_fact add partition (dataset_date='2023-01-04');
alter table src_customer.tran_fact add partition (dataset_date='2023-01-05');
alter table src_customer.tran_fact add partition (dataset_date='2023-01-06');
alter table src_customer.tran_fact add partition (dataset_date='2023-01-07');
alter table src_customer.tran_fact add partition (dataset_date='2023-01-08');
show partitions src_customer.tran_fact;"

# create transaction fact
hive -e 'drop table if exists cards_dw.transaction_fact'
hive -f sql/cards_dw/cd_cards_dw.hql
hive -f sql/cards_dw/ct_transaction_fact.hql



# create table_states
hive -f sql/src_customer/ct_table_states.hql

# fill tables ############################################
# table_states
./load_table_states.sh 2023-01-02
./load_table_states.sh 2023-01-03
./load_table_states.sh 2023-01-04
./load_table_states.sh 2023-01-05
./load_table_states.sh 2023-01-06
./load_table_states.sh 2023-01-07
./load_table_states.sh 2023-01-08

#transaction_fact
./load_transaction_fact.sh 2023-01-02
./load_transaction_fact.sh 2023-01-03
./load_transaction_fact.sh 2023-01-04
./load_transaction_fact.sh 2023-01-05
./load_transaction_fact.sh 2023-01-06
./load_transaction_fact.sh 2023-01-07
./load_transaction_fact.sh 2023-01-08