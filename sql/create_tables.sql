CREATE TABLE dim_customer (
    customer_id VARCHAR(10) PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    join_date DATE,
    loyalty_tier VARCHAR(20),
    updated_at TIMESTAMP DEFAULT GETDATE()
);

CREATE TABLE dim_product (
    product_id VARCHAR(20) PRIMARY KEY,
    product_name VARCHAR(255),
    category VARCHAR(100),
    price DECIMAL(10,2),
    cost DECIMAL(10,2),
    updated_at TIMESTAMP DEFAULT GETDATE()
);

CREATE TABLE sales_fact (
    sale_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    order_date DATE NOT NULL,
    customer_id VARCHAR(10) REFERENCES dim_customer(customer_id),
    product_id VARCHAR(20) REFERENCES dim_product(product_id),
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    total_sale DECIMAL(12,2) NOT NULL,
    payment_method VARCHAR(50),
    region VARCHAR(50),
    processed_at TIMESTAMP DEFAULT GETDATE()
)
DISTKEY(product_id)
SORTKEY(order_date);