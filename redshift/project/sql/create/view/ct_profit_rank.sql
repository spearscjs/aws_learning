/*
    10. Create another view to show total profit by each brand_name,year-mth cd (ex:202201,202203). Do a rank by the total profit (desc) and
    amount of product sold (Asc)
*/

CREATE OR REPLACE VIEW vw.profit_rank AS 
    WITH order_profit_month AS
        (SELECT brand_name, EXTRACT(YEAR FROM sales_date) AS year, 
            EXTRACT(MONTH FROM sales_date) AS month, profit FROM vw.order_profit),
    total_profit_monthly AS
        (SELECT brand_name, year, month, SUM(profit) AS total_profit, COUNT(*) AS num_sold
        FROM order_profit_month
        GROUP BY brand_name, year, month)
    SELECT brand_name, CONCAT(CONCAT(year, '-'), month) AS year_month, total_profit, num_sold,
        RANK() OVER (PARTITION BY year, month ORDER BY total_profit DESC) AS profit_rank, 
        RANK() OVER (PARTITION BY year, month ORDER BY total_profit ASC) AS num_sold_rank FROM total_profit_monthly
        ORDER BY year, month, profit_rank;