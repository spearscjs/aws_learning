CREATE TABLE IF NOT EXISTS lkp_data.lkp_state_details(
    stat_cd varchar(2) encode raw,
    population_cnt int encode AZ64,
    potential_customer_cnt int encode AZ64
);


/*
INSERT INTO lkp_data.lkp_state_details
    (stat_cd, population_cnt, potential_customer_cnt)
    VALUES
        ('NY',200,100),
        ('CA',500,200),
        ('TX',400,300),
        ('NV',100,90),
        ('NJ',200,70);
*/