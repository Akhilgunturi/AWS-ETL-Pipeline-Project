# ETL Pipeline with AWS Glue and Airflow

## Overview
This project demonstrates an ETL pipeline using AWS Glue for transformations and Managed Apache Airflow for orchestration and Redshift for Datawarehouse.

## AWS Services Used
- **S3**: Storage for raw and transformed data
- **Glue**: Serverless ETL service
- **Managed Airflow**: Pipeline orchestration

## Architecture Diagram
![image](https://github.com/user-attachments/assets/8b8991bb-99d6-435b-84b2-d484460ff1d8)

## Components
- **Data Sources**: CSV files in S3
- **ETL Processing**: AWS Glue
- **Orchestration**: Managed Apache Airflow
- **Data Warehouse**: Amazon Redshift

## Pipeline Setup
1. Upload Glue scripts to S3
2. Create Redshift tables
3. Deploy Airflow DAGs
4. Configure connections in Airflow

## Running
The pipeline runs daily automatically
