import os

key = "youtube_project"
iv = "youtube_encyptyo"
salt = "youtube_AesEncryption"

#AWS Access And Secret key
aws_access_key = "Aww4E3p3uNqxNPB764oWoPGbhAqYZ+CscYoOyhO+ZNA="
aws_secret_key = "kt175rS9OJZoxMg8xsIYhuU7EbY6M11Qum8/ri1Bynb3+09dNwzOBpXhcpvUVd/2"
bucket_name = "de-poni"
s3_customer_datamart_directory = "customer_data_mart"
s3_sales_datamart_directory = "sales_data_mart"
s3_source_directory = "sales_data/"
s3_error_directory = "sales_data_error/"
s3_processed_directory = "sales_data_processed/"


#Database credential
# MySQL database connection properties
database_name = "de"
url = f"jdbc:mysql://localhost:3306/de"
properties = {
    "host": "localhost",
    "user": "root",
    "password": "poni",
    "database": "de",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Table name
customer_table_name = "customer"
product_staging_table = "product_staging_table"
product_table = "product"
sales_team_table = "sales_team"
store_table = "store"

#Data Mart details
customer_data_mart_table = "customers_data_mart"
sales_team_data_mart_table = "sales_team_data_mart"

# Required columns
mandatory_columns = ["customer_id","store_id","product_name","sales_date","sales_person_id","price","quantity","total_cost"]


# File Download location
local_directory = "C:\\Users\\samee\\Desktop\\youtube_de_project1\\Data\\file_from_s3\\"
customer_data_mart_local_file = "C:\\Users\\samee\\Desktop\\youtube_de_project1\\Data\\customer_data_mart\\"
sales_team_data_mart_local_file = "C:\\Users\\samee\\Desktop\\youtube_de_project1\\Data\\sales_team_data_mart\\"
sales_team_data_mart_partitioned_local_file = "C:\\Users\\samee\\Desktop\\youtube_de_project1\\Data\\sales_partition_data\\"
error_folder_path_local = "C:\\Users\\samee\\Desktop\\youtube_de_project1\\Data\\error_files\\"
