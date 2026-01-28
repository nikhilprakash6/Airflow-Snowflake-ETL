# Airflow + Snowflake ETL Pipeline (AdventureWorks 2022)

This project implements an end-to-end ETL (Extract, Transform, Load) framework using Apache Airflow, Python, Docker, and Snowflake.
It loads data from AdventureWorks 2022 source views into staging, dimension, and fact tables using a metadata-driven approach. All ETL logic is stored in control tables and executed dynamically through Airflow.

## Project Overview

This ETL pipeline follows the architecture below:

AdventureWorks Views (DATA_ACCESS)  
↓  
Stage Tables  
↓  
Dimension & Fact Tables  
↓  
Reporting Views  

Instead of hardcoding pipelines, all transformation logic is managed using metadata tables. This makes the framework flexible, reusable, and easy to extend.

## Features

- Metadata-driven ETL execution  
- Apache Airflow orchestration  
- Centralized audit and error logging  
- SCD Type 2 implementation for dimensions  
- Docker-based deployment   
- Scalable job execution using job codes  

## Technology Stack

- Apache Airflow  
- Python 3  
- Docker & Docker Compose  
- Snowflake Data Warehouse  
- AdventureWorks 2022 Dataset  
- WSL2 (Linux on Windows)  
- Git & GitHub  

## Project Structure

airflow/  
├── dags/ (Airflow DAG definitions)  
├── include/ (Python ETL runner)  
├── snowflakeSQLfiles/ (SQL setup and job scripts)  
├── docker-compose.yaml (Airflow configuration)  
├── requirements.txt (Python dependencies)  
└── README.md  

## Prerequisites

Before running this project, ensure the following are completed:

1. AdventureWorks 2022 data is loaded into Snowflake  
2. Views are created on all AdventureWorks schemas  
3. All AdventureWorks schemas are prefixed with `MS_`  
4. View database name is `DATA_ACCESS`  
5. All source views are prefixed with `V_`  

   Example:  
   V_PERSON  
   V_SALESORDERHEADER  

6. Docker Desktop and WSL2 are installed and running  
7. A valid Snowflake account is available  

## Setup Instructions

Step 1: Start Airflow

From the project directory, run:
    docker compose up airflow-init  
    docker compose up -d  

Access Airflow UI at:  
http://localhost:8080  

Step 2: Create Data Warehouse Objects

Connect to Snowflake and execute:
    snowflakeSQLfiles/Create-Tables.sql  

This script creates staging tables, dimension tables, fact tables, control tables, audit tables, and sequences.


Step 3: Load ETL Metadata

Execute:
    snowflakeSQLfiles/Master-Table-Inserts.sql  

This script loads ETL logic, source-to-target mappings, job configurations, and step definitions into the master control table.
Each job represents a dimension or fact load process.


## How the ETL Process Works

1. Metadata Configuration  
All ETL logic is stored in ADWBI_EXECUTE_PROCEDURES_MASTER_TABLE.  
Each record defines job code, step number, SQL logic, source details, and target details.

2. Airflow Orchestration  
Airflow triggers ETL jobs using job codes (for example: JOB_01).  
Each job contains multiple ordered execution steps.

3. Python ETL Runner  
A custom Python runner executes the pipeline by reading job metadata, generating run IDs, executing SQL logic, handling multi-step operations, capturing execution status, and logging errors.  
This replaces traditional stored procedures.

4. Data Loading Flow  
For each job, source data is extracted from DATA_ACCESS views, loaded into staging tables, transformed, and then loaded into dimension and fact tables. Reporting views are refreshed.  
SCD Type 2 logic is applied to preserve historical records.

5. Audit and Monitoring  
Execution details are recorded in ADWBI_AUDIT_ERROR_LOG_TABLE, including run ID, step number, start and end time, load status, and error messages.

## Running the Pipeline

To run an ETL job:

1. Open Airflow UI  
2. Enable DAG: adwbi_etl_jobs  
3. Click Trigger  
4. Monitor execution logs  

Each job loads one dimension or fact table.
