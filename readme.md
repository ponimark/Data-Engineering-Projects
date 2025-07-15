# **Data Engineering Pipeline – Sales & Customer Analytics**

## **📌 Project Overview**

This project implements an **end-to-end ETL pipeline** using **PySpark**, **AWS S3**, and **MySQL** for processing sales and customer data. It validates incoming CSV files, enriches them with dimension tables, calculates key business metrics, and generates data marts for analytical use.

---

## **🛠️ Key Features**

✔ **Automated CSV Ingestion & Validation**  
✔ **Schema Validation (Mandatory Columns Check)**  
✔ **Error Handling & Rejected Files Processing (moved to `rejected_csv` in S3)**  
✔ **Data Enrichment using Dimension Tables**  
✔ **Customer & Sales Team Data Marts Generation**  
✔ **Partitioned Parquet Storage for Analytical Workloads**  
✔ **Automatic S3 File Movement (Raw → Processed)**  
✔ **Incremental Status Updates in MySQL Staging Table**

---

## **📂 Project Workflow**

### **1. Input Validation**
- Reads CSV files from **AWS S3 (raw source directory)**  
- Checks mandatory columns against config  
- Moves incorrect files to `rejected_csv/` in S3

### **2. Staging Table Updates**
- Inserts validated files into **MySQL staging table** with status `'A'` (active)  
- Updates processed files to status `'I'` (inactive)

### **3. Data Transformation**
- Joins fact data with dimension tables (Customer, Store, Sales Team)  
- Handles extra columns by consolidating them into an `additional_column`

### **4. Data Marts**
- **Customer Data Mart:** Monthly purchase summary for customers  
- **Sales Team Data Mart:** Incentive and sales performance calculations  
- Stores results locally as Parquet → uploads to S3

### **5. Partitioned Data for Analytics**
- Saves **partitioned parquet** by `sales_month` & `store_id`  
- Uploads partitioned data to S3 with timestamped folder structure

### **6. Calculations**
- Monthly amount purchased per customer  
- Incentive calculations for sales team

---

## **⚙️ Technologies Used**

- **Python 3.x**
- **PySpark (ETL, Transformations)**
- **AWS S3 (Raw, Rejected, Processed, Data Marts)**
- **MySQL (Staging & Dimension Tables)**
- **Boto3 (AWS S3 operations)**

---

Requirement to complete the projects:-
1. You should have laptop with minimum 4 GB of RAM, i3 and above (Better to have 8GB with i5).
2. PyCharm installed in the system.
4. MySQL workbench should also be installed to the system.
5. GitHub account is good to have but not necessary.
5. You should have AWS account.
6. Understanding of spark,sql and python is required.

```plaintext
Project structure:-
my_project/
├── docs/
│   └── readme.md
├── resources/
│   ├── __init__.py
│   ├── dev/
│   │    ├── config.py
│   │    └── requirement.txt
│   └── qa/
│   │    ├── config.py
│   │    └── requirement.txt
│   └── prod/
│   │    ├── config.py
│   │    └── requirement.txt
│   ├── sql_scripts/
│   │    └── table_scripts.sql
├── src/
│   ├── main/
│   │    ├── __init__.py
│   │    └── delete/
│   │    │      ├── aws_delete.py
│   │    │      ├── database_delete.py
│   │    │      └── local_file_delete.py
│   │    └── download/
│   │    │      └── aws_file_download.py
│   │    └── move/
│   │    │      └── move_files.py
│   │    └── read/
│   │    │      ├── aws_read.py
│   │    │      └── database_read.py
│   │    └── transformations/
│   │    │      └── jobs/
│   │    │      │     ├── customer_mart_sql_transform_write.py
│   │    │      │     ├── dimension_tables_join.py
│   │    │      │     ├── main.py
│   │    │      │     └──sales_mart_sql_transform_write.py
│   │    └── upload/
│   │    │      └── upload_to_s3.py
│   │    └── utility/
│   │    │      ├── encrypt_decrypt.py
│   │    │      ├── logging_config.py
│   │    │      ├── s3_client_object.py
│   │    │      ├── spark_session.py
│   │    │      └── my_sql_session.py
│   │    └── write/
│   │    │      ├── database_write.py
│   │    │      └── parquet_write.py
│   ├── test/
│   │    ├── scratch_pad.py.py
│   │    └── generate_csv_data.py
```

How to run the program in Pycharm:-
1. Open the pycharm editor.
2. Upload or pull the project from GitHub.
3. Open terminal from bottom pane.
4. Goto virtual environment and activate it. Let's say you have venv as virtual environament.i) cd venv ii) cd Scripts iii) activate (if activate doesn't work then use ./activate)
5. Create main.py as explained in my videos on YouTube channel.
6. You will have to create a user on AWS also and assign s3 full access and provide secret key and access key to the config file.
6. Run main.py from green play button on top right hand side.
7. If everything works as expected enjoy, else re-try.

