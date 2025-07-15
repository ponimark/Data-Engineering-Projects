from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from resources.dev import config
from src.main.write.database_write import DatabaseWriter


def sales_mart_calculation_table_write(table_name):
    window=Window.partitionBy('store_id','sales_person_id','sales_month')
    window_table=table_name\
                    .withColumn("sales_date_month",
                                           to_date(concat(substring(col("sales_date"), 1, 7),
                                                          lit("-01")),
                                                   "yyyy-MM-dd"))\
                    .withColumn('total_sales_every_month_by_every_sales_person',
                                sum(col('total_cost')).over(window))\
                    .select('store_id','sales_person_id',
                            concat(col("sales_person_first_name"),lit(" "),col("sales_person_last_name")).alias("sales_person_full_name"),
                            'sales_month','total_sales_every_month_by_every_sales_person').distinct()

    rank_window = Window.partitionBy('store_id','sales_month')\
                 .orderBy(
                    col('total_sales_every_month_by_every_sales_person').desc())

    final_table=window_table\
                .withColumn('rnk', rank().over(rank_window))\
                .filter(col('rnk')==1)\
                .withColumn('incentive',col('total_sales_every_month_by_every_sales_person')*0.01)\
                                                .withColumn('incentive',round(col('incentive'),2))\
                            .select('store_id','sales_person_id','sales_person_full_name',
                                    'sales_month','total_sales_every_month_by_every_sales_person','incentive')

    final_table.show()

    print("WRITING DATA INTO MYSQL SALES_DATA_MART")
    db_writer=DatabaseWriter(url=config.url, properties=config.properties)
    db_writer.write_dataframe(final_table,config.sales_team_data_mart_table)

