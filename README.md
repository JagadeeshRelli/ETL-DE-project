# Python-Driven ETL with Airflow & PostgreSQL on Docker


Context : Design a Wallet service which receives million transactions every day.  
Design an ETL Pipeline that supports upserts and append operations to sync this data to data warehouse.
Updates can be days old at times.   
These million transactions are a combination of  

new transactions  
updates from older transactions  

Summary: 
-Built a Python-driven ETL pipeline to extract, transform, and clean data using Pandas DataFrames.  
-Loaded data into PostgreSQL with upsert operations for efficient updates and inserts.  
-Orchestrated and scheduled the ETL process using Apache Airflow in a Dockerized environment.  
