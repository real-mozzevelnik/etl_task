CREATE OR REPLACE TABLE products_categories (
    product_id UInt32,
    product_name String,
    category_id UInt32,
    category_name LowCardinality(String),
) 
ENGINE = ReplacingMergeTree()
ORDER BY (product_id);