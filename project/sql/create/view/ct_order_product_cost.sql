/*
class_question6.4:  create a view by joining order and product_cost.
*/
CREATE OR REPLACE VIEW vw.order_product_cost AS 
    SELECT o.*, c.manufacturing_cost FROM cards_ingest.order o
    JOIN cards_ingest.product_cost c
    ON o.product_name = c.product_name;