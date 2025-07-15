import os
import shutil
from datetime import datetime
from logging import exception
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType
from src.main.delete.local_file_delete import *
from src.main.transformations.jobs.customer_mart_sql_tranform_write import customer_mart_calculation_table_write
from src.main.transformations.jobs.dimension_tables_join import *
from resources.dev import config
from src.main.move.move_files import move_s3_to_s3
from src.main.read.database_read import DatabaseReader
from src.main.transformations.jobs.sales_mart_transformation import sales_mart_calculation_table_write
from src.main.upload.upload_to_s3 import UploadToS3
from src.main.utility.encrypt_decrypt import *
from src.main.utility.my_sql_session import *
from src.main.utility.s3_client_object import *
from src.main.utility.logging_config import *
from resources.dev.config import properties, error_folder_path_local
from src.main.read.aws_read import *
from src.main.utility.spark_session import spark_session
from src.main.write.parquet_writer import ParquetWriter
from src.test.scratch_pad import s3_absolute_file_path
from src.main.download.aws_file_download import *

aws_access_key=config.aws_access_key
aws_secret_key=config.aws_secret_key

s3_client_provider=S3ClientProvider(decrypt(aws_access_key), decrypt(aws_secret_key))
s3_client=s3_client_provider.get_client()

response=s3_client.list_buckets()
logger.info("List of Buckets: %s", response['Buckets'])

# check if local directory already has a file
# if file is present in the staging area with status as A if so try to re-run

csv_files= [file for file in os.listdir(config.local_directory) if file.endswith(".csv")]
connection=get_mysql_connection()
cursor=connection.cursor()

total_csv_files=[]
if csv_files:
    for file in csv_files:
        total_csv_files.append(file)

    file_list_str = ",".join(f"'{f}'" for f in total_csv_files)
    statement = f"""SELECT DISTINCT file_name 
                    FROM {config.properties['database']}.{config.product_staging_table}
                    WHERE file_name IN ({file_list_str}) AND status='A'"""

    logger.info(f"dynamically statement created: {statement}")
    cursor.execute(statement)
    data=cursor.fetchall()
    if data:
        logger.info("Your last run was failed please check")
    else:
        logger.info("No data found")

else:
    logger.info("Last run was successful!!!")


try:
    s3_reader=S3Reader()

    folder_path= config.s3_source_directory
    s3_absolute_file_path=s3_reader.list_files(s3_client,config.bucket_name,folder_path=folder_path)
    logger.info("Absolute file path on s3 bucket for csv file %s ",s3_absolute_file_path)
    if not s3_absolute_file_path:
        logger.info(f"No file available at {folder_path}")
        raise Exception("No Data available to process ")

except Exception as e:
    logger.error("Exited with error:- %s",e)
    raise e


bucket_name=config.bucket_name
local_directory=config.local_directory

prefix= f"s3://{bucket_name}/"
file_paths=[url[len(prefix):] for url in s3_absolute_file_path]
logger.info("File path available on s3 under %s bucket and folder is %s",bucket_name,prefix)
logging.info(f"File path available on s3 under {bucket_name} bucket and folder is {prefix}")

try:
    downloader=S3FileDownloader(s3_client,bucket_name, local_directory)
    downloader.download_files(file_paths)

except Exception as e:
    logger.error("Download error :- %s",e)
    sys.exit()


all_files=os.listdir(local_directory)
logger.info(f"List of file present after download: {all_files}")

if all_files:
    csv_files=[]
    error_files=[]
    for files in all_files:
        if files.endswith(".csv"):
            csv_files.append(os.path.abspath(os.path.join(local_directory,files)))
        else:
            error_files.append(os.path.abspath(os.path.join(local_directory,files)))

    if not csv_files:
        logger.error("No CSV Data to process")
        raise exception("No CSV Data to process")

else:
    logger.error("No Data to process")
    raise Exception("No Data to process")


logger.info("******************** Listing the file ************************")
logger.info(f"List of file present after processing: {csv_files}")
logger.info("******************** Creating Spark Session *******************")

spark=spark_session()

logger.info("********************* Spark session created **************************")


correct_files=[]
for data in csv_files:
    data_schema=spark.read.format('csv')\
                .option('header','true')\
                .load(data).columns
    logger.info(f"Schema for {data} is {data_schema}")
    logger.info(f"Mandatory Columns for correct data are {config.mandatory_columns}")
    missing_columns=set(config.mandatory_columns) - set(data_schema)

    if missing_columns:
        error_files.append(data)

    else:
        logger.info(f"NO MISSING COLUMN IN {data}")
        correct_files.append(data)

if error_files:
    for file_path in error_files:
        if os.path.exists(file_path):
            file_name=os.path.basename(file_path)
            destination_path=os.path.join(error_folder_path_local,file_name)

            shutil.move(file_path,destination_path)
            logger.info(f"Moved {file_name} from s3 file path to {destination_path}")

            source_prefix=config.s3_source_directory
            destination_prefix=config.s3_error_directory

            message=move_s3_to_s3(s3_client,config.bucket_name,source_prefix,destination_prefix,destination_path)
            logger.info(f"{message}")

        else:
            logger.error(f"File path {file_path} does not exist")

else:
    logger.info("******************No error files present after processing*********************")



#INSERTING INTO TABLE

logger.info("**********Inserting into TABLE *********************")

insert_statements=[]
db_name=config.database_name
current_date=datetime.now()
formatted_date=current_date.strftime("%Y-%m-%d %H:%M:%S")

if correct_files:
    for file in correct_files:
        filename=os.path.basename(file)
        statement=f"""INSERT INTO {db_name}.{config.product_staging_table}\
                    (file_name,created_date, status)\
                    VALUES ('{filename}', '{formatted_date}','A')"""

        insert_statements.append(statement)
    logger.info(f"Insert statement for tables : {insert_statements}")
    logger.info(f"***************CONNECTING TO DATABASE *******************")
    connection=get_mysql_connection()
    cursor=connection.cursor()
    logger.info(f"************ SUCCESSFULLY CONNECTED TO DATABASE ***************")
    for statement in insert_statements:
        cursor.execute(statement)
        connection.commit()

    cursor.close()
    connection.close()

else:
    logger.error("NO FILE TO PROCESS")
    raise Exception("NO DATA WITH CORRECT FILES")

#FIXING EXTRA COLUMNS
# try:
#     schema= StructType([
#         StructField("customer_id", IntegerType(), True),
#         StructField("store_id", IntegerType(), True),
#         StructField("product_name", StringType(), True),
#         StructField("sales_date", DateType(), True),
#         StructField("sales_person_id", IntegerType(), True),
#         StructField("price",FloatType(), True),
#         StructField("quantity", IntegerType(), True),
#         StructField("total_cost", FloatType(), True),
#         StructField("additional_column", StringType(), True),
#     ])
#
#     final_df_to_process=spark.createDataFrame([],schema=schema)
#     final_df_to_process.show()
#
# except Exception as e:
#     logger.error("Error while Creating Dataframe :- %s",e)
#     sys.exit()

#If error in creating dataframe form spark will have to create table form MYSQL

database_client= DatabaseReader(config.url,config.properties)
logger.info("************* CREATING EMPTY DATAFRAME **********************")
final_df_to_process=database_client.create_dataframe(spark, "empty_df_create_table")
final_df_to_process.show()

for data in correct_files:
    data_df=spark.read.format('csv')\
                      .option('header','true')\
                      .option('inferSchema','true')\
                      .load(data)
    data_schema=data_df.columns
    extra_columns=list(set(data_schema)-set(config.mandatory_columns))
    logger.info(f"ADDITIONAL COLUMNS AT SOURCE : {extra_columns} in {os.path.basename(data)} table")
    if extra_columns:
        data_df=data_df.withColumn("additional_column",concat_ws(", ",*extra_columns))\
            .select('customer_id','store_id','product_name','sales_date','sales_person_id','price',
                    'quantity','total_cost','additional_column')
        logger.info(f"PROCESSED {data} AND ADDED 'ADDITIONAL COLUMN' ")

    else:
        data_df=data_df.withColumn('additional_column',lit(None)) \
                .select('customer_id', 'store_id', 'product_name', 'sales_date', 'sales_person_id', 'price',
                       'quantity','total_cost', 'additional_column')

    final_df_to_process=final_df_to_process.union(data_df)

logger.info("****************** FINAL DATAFRAME FROM SOURCE TO MOVE AHEAD *********************")
final_df_to_process.show()
print("Counting rows...")
print(final_df_to_process.count())
final_df_to_process.printSchema()




database_client= DatabaseReader(config.url,config.properties)

logger.info("************* CONVERTING CUSTOMER TABLE INTO CUSTOMER_DF (DATAFRAME)")
customer_df=database_client.create_dataframe(spark, config.customer_table_name)

logger.info("************* CONVERTING PRODUCT TABLE INTO PRODUCT_DF (DATAFRAME)")
product_df=database_client.create_dataframe(spark,config.product_table)

logger.info("************* CONVERTING PRODUCT_STAGING_TABLE INTO PRODUCT_STAGING_DF (DATAFRAME)")
product_staging_df=database_client.create_dataframe(spark,config.product_staging_table)

logger.info("************* CONVERTING SALES_TEAM TABLE INTO SALES_TEAM_DF (DATAFRAME)")
sales_team_df=database_client.create_dataframe(spark,config.sales_team_table)

logger.info("************* CONVERTING STORE TABLE INTO STORE_DF (DATAFRAME)")
store_df=database_client.create_dataframe(spark,config.store_table)


s3_customer_store_sales_df_join= dimensions_table_join(final_df_to_process,
                                                      customer_df,
                                                      store_df,
                                                      sales_team_df)

logger.info("************* FINAL ENRICHED DATA *****************")
s3_customer_store_sales_df_join.show()

s3_customer_store_sales_df_join.printSchema()


#DATA WILL BE WRITTEN IN LOCAL FIRST THEN UPLOADED TO S3 FOR ANALYTICS, ALSO WILL BE LOADED TO MYSQL

#WRITTING INTO CUSTOMER DATA MART
logger.info("************* WRITE DATA TO CUSTOMER DATA MART *******************")

final_customer_data_mart_df= s3_customer_store_sales_df_join\
                            .select('customer_id', 'ct.first_name', 'ct.last_name','ct.address','ct.pincode',
                                    'phone_number','sales_date','total_cost')


parquet_writer=ParquetWriter('overwrite','parquet')
parquet_writer.dataframe_writer(final_customer_data_mart_df,config.customer_data_mart_local_file)

logger.info("********* FINAL DATA FOR DALES DATA MART *******************")
final_customer_data_mart_df.show()


logger.info(f"************* CUSTOMER DATA WRITTEN TO LOCAL DISK AT {config.customer_data_mart_local_file} ***************")

#LOCAL TO S3

logger.info("***************************** LOCAL TO S3 ****************************")

s3_uploader=UploadToS3(s3_client)
s3_directory=config.s3_customer_datamart_directory
message = s3_uploader.upload_to_s3(s3_directory,config.bucket_name,config.customer_data_mart_local_file)
logger.info(f"{message}")

#SALES TEAM DATA MART

logger.info("************* WRITE DATA TO SALES TEAM DATA MART *******************")

final_sales_team_data_mart_df= s3_customer_store_sales_df_join\
                            .select('store_id', 'sales_person_id', 'sales_person_first_name',
                                    'sales_person_last_name','store_manager_name','manager_id',
                                    'is_manager','sales_person_address','sales_person_pincode',
                                    'sales_date','total_cost',expr('substring(sales_date,1,7)as sales_month'))



parquet_writer.dataframe_writer(final_sales_team_data_mart_df,config.sales_team_data_mart_local_file)

logger.info("********* FINAL DATA FOR DALES DATA MART *******************")
final_sales_team_data_mart_df.show()

logger.info(f"************* SALES TEAM DATA WRITTEN TO LOCAL DISK AT {config.sales_team_data_mart_local_file} ***************")


# LOCAL TO S3 FOR SALES DATA MART

s3_directory=config.s3_sales_datamart_directory
message = s3_uploader.upload_to_s3(s3_directory,config.bucket_name,config.sales_team_data_mart_local_file)
logger.info(f"{message}")


#WRITING DATA INTO PARTITIONS

final_sales_team_data_mart_df.write.format('parquet')\
                                    .option('header', 'true')\
                                    .mode('overwrite')\
                                    .partitionBy('sales_month','store_id')\
                                    .option('path',config.sales_team_data_mart_partitioned_local_file)\
                                    .save()

logger.info(f"********************* PARTITIONED DATA SAVED AT {config.sales_team_data_mart_partitioned_local_file} ***************")
#UPLOADING PARTITIONED DATA TO S3

s3_prefix='sales_partitioned_data_mart'
current_epoch=int(datetime.now().timestamp())*1000
for root, dirs, files in os.walk(config.sales_team_data_mart_partitioned_local_file):
    for file in files:
        print(file)
        local_file_path=os.path.join(root,file)
        relative_file_path=os.path.relpath(local_file_path,config.sales_team_data_mart_partitioned_local_file)

    s3_key=f"{s3_prefix}/{current_epoch}/{relative_file_path}"
    s3_client.upload_file(local_file_path,config.bucket_name,s3_key)

    #CALCULATIONS


logger.info("***************** CALCULATION FOR CUSTOMER EVERY MONTH AMOUNT PURCHASED ****************")
customer_mart_calculation_table_write(final_customer_data_mart_df)

logger.info("************ CALCULATION FOR SALES PERSON INCENTIVE ************************")
sales_mart_calculation_table_write(final_sales_team_data_mart_df)

# MOVE FILES TO S3 INTO PROCESSED FOLDER AND DELETE FROM LOCAL

source_prefix=config.s3_source_directory
destination_prefix=config.s3_processed_directory
message = move_s3_to_s3(s3_client,config.bucket_name,source_prefix,destination_prefix)
logger.info(f"{message}")


# logger.info('************** DELETING FILES FROM LOCAL **************')
# delete_local_file(config.local_directory)
# logger.info('************** DELETED FILES FROM LOCAL **************')
#
#
# logger.info('************** DELETING SALES FILES FROM LOCAL **************')
# delete_local_file(config.sales_team_data_mart_local_file)
# logger.info('************** DELETED SALES FILES FROM LOCAL **************')
#
# logger.info('************** DELETING CUSTOMER FILES FROM LOCAL **************')
# delete_local_file(config.customer_data_mart_local_file)
# logger.info('************** DELETED CUSTOMER FILES FROM LOCAL **************')
#
#
# logger.info('************** DELETING SALES PARTITIONED FILES FROM LOCAL **************')
# delete_local_file(config.customer_data_mart_local_file)
# logger.info('************** DELETED SALES PARTITIONED FILES FROM LOCAL **************')


# UPDATE STATUS OF STAGING TABLE

update_statement=[]
if correct_files:
    for file in correct_files:
        filename=os.path.basename(file)
        statement=f" UPDATE {db_name}.{config.product_staging_table}"\
                  f" SET status = 'I',updated_date='{formatted_date}'"\
                  f"WHERE file_name = '{filename}'"

        update_statement.append(statement)

    logger.info(f"UPDATED STATEMENT CREATED FOR STAGING TABLE ------ {update_statement}")
    logger.info("**************** CONNECTING TO MYSQL ********************")
    connection=get_mysql_connection()
    cursor=connection.cursor()
    logger.info(f"*********************** CONNECTED TO MYSQL CONNECTION *********************")

    for statement in update_statement:
        cursor.execute(statement)
        connection.commit()
    cursor.close()
    connection.close()
    
else:
    logger.error("************* SOME ERROR ****************")
    sys.exit()


input("Press to terminate")
