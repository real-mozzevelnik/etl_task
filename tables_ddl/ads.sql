CREATE OR REPLACE TABLE ads (
    date Date,
    product_id UInt32,
    advertising_amount Float32
) 
ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (date, product_id);