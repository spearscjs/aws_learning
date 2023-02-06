CREATE TABLE IF NOT EXISTS lkp_data.lkp_state_details(
    stat_cd varchar(2) encode raw,
    population_cnt int encode AZ64,
    potential_customer_cnt int encode AZ64
);