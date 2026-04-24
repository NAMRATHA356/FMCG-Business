FMCG Data Engineering Pipeline (Medallion Architecture)
Overview

This project implements an end-to-end data pipeline using the Medallion Architecture (Bronze, Silver, Gold) on Databricks. It processes FMCG domain data such as products, customers, pricing, and orders to generate business-ready datasets and insights.

The pipeline transforms raw data into structured, clean, and analytics-ready data for reporting and dashboarding.

Architecture
Bronze Layer (Raw Data)
Stores raw ingested data from source systems (CSV files in S3)
No transformations applied
Adds metadata columns:
read_timestamp
file_name
file_size

Example:

Products →
Pricing →
Customers →
Orders →
Silver Layer (Cleaned & Transformed Data)
Data cleaning and standardization
Business logic applied
Ensures data quality
Key Transformations

Products

Remove duplicates
Fix category formatting
Correct spelling errors (Protien → Protein)
Generate product_code
Create division, variant

Pricing

Normalize date formats
Validate and clean gross_price
Join with products to get product_code

Customers

Remove duplicates
Fix city names and invalid values
Trim and standardize customer names
Create derived fields:
customer
market
platform
channel

Orders

Clean date formats
Remove invalid records
Deduplicate data
Join with products to get product_code
Gold Layer (Business Ready Data)
Optimized for analytics and reporting
Aggregated and structured tables
Tables Created
dim_products
dim_customers
dim_gross_price
fact_orders
Features
Monthly aggregation of sales
Latest product pricing per year
Clean dimensional modeling