# E-commerce ETL and Data Quality Project
A comprehensive ETL (Extract, Transform, Load) pipeline for processing payment data, leveraging AWS S3, AWS Glue, AWS Redshift, and Amazon Quicksight. <br>

### Overview
This project implements an end-to-end data pipeline for payment processing and analytics. It includes components for: <br>
- Local processing using Python Scripts
- Cloud-based ETL using AWS Glue
- Data warehousing with Amazon Redshift
- Visualization with Amazon QuickSight

### Project Structure
```
payment-etl-pipeline/
│
├── data/                      # Data storage
│   ├── raw/                   # Generated raw payment data
│   └── processed/             # Processed data (local testing)
│
├── scripts/                   # Local scripts
│   ├── local_etl_test.py      # Python ETL for local testing
│   └── upload_to_s3.py        # Uploads data to S3
│
├── aws/                       # AWS components
│   ├── glue/                  # AWS Glue ETL resources
│   ├── redshift/              # Redshift setup and queries
│   └── quicksight/            # QuickSight dashboard
│
├── notebooks/                 # Jupyter notebooks
│   └── data_exploration.ipynb
│
└── docs/                      # Documentation
    └── architecture.md        # Data Schema and E/R diagram
```


