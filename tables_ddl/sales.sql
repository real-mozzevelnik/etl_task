CREATE OR REPLACE TABLE sales (
    date Date,
    product_id UInt32,
    order_id UInt64,
    sales_amount Float32
) 
ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (date, product_id);