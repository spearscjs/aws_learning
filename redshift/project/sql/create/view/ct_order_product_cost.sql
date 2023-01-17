CREATE OR REPLACE VIEW vw.order_product_cost AS 
    SELECT o.*, c.manufacturing_cost FROM cards_ingest.order o
    JOIN cards_ingest.product_cost c
    ON o.product_name = c.product_name;