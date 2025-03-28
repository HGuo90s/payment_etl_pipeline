-- AWS Redshift Create Tables SQL
-- This script creates a star schema with dimension and fact tables
-- for a data warehouse in Amazon Redshift

-- Create dimension tables first, then the fact table

-- Customer dimension
CREATE TABLE dim_cust (
    customer_id INTEGER NOT NULL PRIMARY KEY,
    customer_name VARCHAR(100),
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    create_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_date TIMESTAMP
)
DISTSTYLE KEY
DISTKEY (customer_id)
SORTKEY (customer_id);

-- Date dimension
CREATE TABLE dim_date (
    date_full DATE NOT NULL PRIMARY KEY,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(10) NOT NULL,
    day_of_month INTEGER NOT NULL,
    day_of_year INTEGER NOT NULL,
    week_of_year INTEGER NOT NULL,
    month_num INTEGER NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    quarter INTEGER NOT NULL,
    year INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL
)
DISTSTYLE ALL
SORTKEY (date_full);

-- Employee dimension
CREATE TABLE dim_emp (
    employee_id INTEGER NOT NULL PRIMARY KEY,
    employee_name VARCHAR(100),
    employee_first_name VARCHAR(50),
    employee_last_name VARCHAR(50),
    create_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_date TIMESTAMP
)
DISTSTYLE ALL
SORTKEY (employee_id);

-- Geography dimension
CREATE TABLE dim_geo (
    state_id VARCHAR(40) NOT NULL PRIMARY KEY,
    state_code VARCHAR(2) NOT NULL,
    country VARCHAR(30) NOT NULL,
    state_name VARCHAR(50) NOT NULL,
    capital_city VARCHAR(50),
    status VARCHAR(30),
    iso_code VARCHAR(10)
)
DISTSTYLE ALL
SORTKEY (state_id);

-- Order status dimension
CREATE TABLE dim_ostatus (
    status_id INTEGER NOT NULL PRIMARY KEY,
    status_name VARCHAR(15) NOT NULL,
    status_description VARCHAR(50),
    create_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_date TIMESTAMP
)
DISTSTYLE ALL
SORTKEY (status_id);

-- Product dimension
CREATE TABLE dim_prod (
    product_id INTEGER NOT NULL PRIMARY KEY,
    product_name VARCHAR(30) NOT NULL,
    category VARCHAR(15) NOT NULL,
    brand VARCHAR(20) NOT NULL,
    standard_cost FLOAT NOT NULL,
    create_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_date TIMESTAMP
)
DISTSTYLE KEY
DISTKEY (product_id)
SORTKEY (product_id);

-- Orders fact table
CREATE TABLE fact_orders (
    order_number INTEGER NOT NULL PRIMARY KEY,
    order_date DATE NOT NULL REFERENCES dim_date(date_full),
    customer_id INTEGER NOT NULL REFERENCES dim_cust(customer_id),
    state_id VARCHAR(40) NOT NULL REFERENCES dim_geo(state_id),
    product_id INTEGER NOT NULL REFERENCES dim_prod(product_id),
    status_id INTEGER NOT NULL REFERENCES dim_ostatus(status_id),
    employee_id INTEGER NOT NULL REFERENCES dim_emp(employee_id),
    unit_cost FLOAT NOT NULL,
    unit_sales FLOAT NOT NULL,
    quantity INTEGER NOT NULL,
    total_cost FLOAT NOT NULL,
    total_sales FLOAT NOT NULL,
    profit FLOAT NOT NULL,
    profit_margin FLOAT NOT NULL
)
DISTSTYLE KEY
DISTKEY (product_id)
SORTKEY (order_date, product_id);

-- Add constraints for data integrity
ALTER TABLE fact_orders ADD CONSTRAINT check_quantity_positive CHECK (quantity > 0);
ALTER TABLE fact_orders ADD CONSTRAINT check_profit_margin CHECK (profit_margin >= -1 AND profit_margin <= 1);
ALTER TABLE dim_date ADD CONSTRAINT check_day_of_week CHECK (day_of_week BETWEEN 1 AND 7);
ALTER TABLE dim_date ADD CONSTRAINT check_month_num CHECK (month_num BETWEEN 1 AND 12);
ALTER TABLE dim_date ADD CONSTRAINT check_quarter CHECK (quarter BETWEEN 1 AND 4);
