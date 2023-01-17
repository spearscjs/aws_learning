/*
    8. Create col as profit_group the values are [ if profit percenatge is more 10% the "Bumper profit", it is 0 to 10% then "Marginal profit"
    if it is -5% to 0% then "Loss making" else "Bumper Loss"
    # 7. Create a new view by taking all the records where you have profit. Profit is sales amount -manufacturingcost. So add all the fields from order table
    # and cost from product_cost.
*/
CREATE OR REPLACE VIEW vw.order_profit AS 
(select *, sales_ammount - manufacturing_cost AS profit, 
        ROUND(((sales_ammount-manufacturing_cost)/manufacturing_cost),4) * 100 AS profit_percentage,
        CASE 
            WHEN ROUND(((sales_ammount-manufacturing_cost)/manufacturing_cost),4) * 100 > 10 THEN 'Bumper profit'
            WHEN ROUND(((sales_ammount-manufacturing_cost)/manufacturing_cost),4) * 100 >= 0 THEN 'Marginal profit'
            WHEN ROUND(((sales_ammount-manufacturing_cost)/manufacturing_cost),4) * 100 >= -5 THEN 'Loss making'
            ELSE 'Bumper Loss'
        END AS profit_group
        FROM vw.order_product_cost);
