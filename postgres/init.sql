-- Initialization script for PostgreSQL
-- This will run automatically when the database container starts for the first time

-- Create a sample schema
CREATE SCHEMA IF NOT EXISTS sample_data;

-- Set up a raw data table
CREATE TABLE IF NOT EXISTS sample_data.raw_orders (
    order_id SERIAL PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL,
    product_name VARCHAR(100) NOT NULL,
    quantity INT NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Set up a processed data table for the ETL output
CREATE TABLE IF NOT EXISTS sample_data.processed_orders (
    order_id INT PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL,
    product_name VARCHAR(100) NOT NULL,
    total_amount DECIMAL(10, 2) NOT NULL,
    processed_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert some sample raw data
INSERT INTO sample_data.raw_orders (customer_name, product_name, quantity, price) VALUES
    ('Alice Smith', 'Laptop', 1, 1200.00),
    ('Bob Jones', 'Mouse', 2, 25.50),
    ('Charlie Brown', 'Keyboard', 1, 75.00),
    ('Diana Prince', 'Monitor', 2, 300.00),
    ('Evan Wright', 'Desk Chair', 1, 150.00);

-- Enable pgvector extension
CREATE EXTENSION IF NOT EXISTS vector;
