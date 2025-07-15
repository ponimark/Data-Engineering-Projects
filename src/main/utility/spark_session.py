import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from src.main.utility.logging_config import *

def spark_session():
    spark = SparkSession.builder.master("local[*]") \
        .appName("project")\
        .getOrCreate()
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", "AKIAWHUDXLOCUGFVY2WJ")
    hadoop_conf.set("fs.s3a.secret.key", "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")
    hadoop_conf.set("fs.s3a.endpoint", "s3.amazonaws.com")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    logger.info("spark session %s",spark)
    return spark