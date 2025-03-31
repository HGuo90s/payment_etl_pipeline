# AWS Glue ETL Job - E-commerce Data Warehouse
This repository contains an AWS Glue ETL job that transforms raw e-commerce data into a star schema data warehouse model ([Data Schema](https://github.com/HGuo90s/payment_etl_pipeline/blob/main/docs/architecture.md)). The job extracts data from a raw source, processes dimension and fact tables, and loads the results to S3 in Parquet format.

### Overview
The ETL job creates the following dimensional model: <br>
Fact Table: Orders (sales transactions)
Dimension Tables:
- Date
- Customer
- Geography (States/Provinces from India, US, and Canada)
- Product
- Order Status
- Employee/Supervisor

### Prerequisites
AWS Account with appropriate permissions <br>
AWS Glue service enabled <br>
S3 bucket for storing processed data <br>
Glue Catalog database with source data table <br>

### Data Sources and Targets
Source Database: raw_ecommerce_db <br>
Source Table: online_ecommerce_csv <br>
Target S3 Bucket: aws-bucket-ecommerce <br>
Target S3 Folder: processed/ <br>
The source and target location are variables and may be edited. 

### Script Structure
The ETL script is organized into several key functions: <br>
data_preprocessing(): Cleans and prepares the raw data <br>
proc_date_dim(): Creates the date dimension table <br>
proc_cust_dim(): Creates the customer dimension table <br>
proc_geo_dim(): Creates the geography dimension table <br>
proc_prod_dim(): Creates the product dimension table <br>
proc_ostatus_dim(): Creates the order status dimension table <br>
proc_emp_dim(): Creates the employee dimension table <br>
fact_table(): Creates the fact table with foreign keys to dimensions <br>
save_dfs_to_s3(): Saves the processed tables to S3 in Parquet format <br>


### Customization
To adapt this script for your own data: <br>
Update the database and table names in the main() function <br>
Modify the S3 bucket and folder path <br>
Adjust the column mappings if your source data has different column names <br>
Update the geography dimension if you need different regions <br>





