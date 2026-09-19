from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql import functions as func
from pyspark.sql.functions import expr
from datetime import date , timedelta
import re
import sys
import subprocess
RUN_DATE = date.today()
PARENT_DIR = f"date={RUN_DATE}"
TRANSFORMATION_SILVER_PATH ="/user/linux/silver/transformations"
GOLD_PATH = "/user/linux/gold"
TRAFFIC_TABLE_PATH = f"{GOLD_PATH}/traffic_table/{PARENT_DIR}"
REQUESTS_TABLE_PATH = f"{GOLD_PATH}/requests_table/{PARENT_DIR}"



spark = SparkSession.builder.appName("Log Analysis Pipeline").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")




cleansed_data = spark.read.parquet(f"hdfs://localhost:9000/{TRANSFORMATION_SILVER_PATH}/{PARENT_DIR}")




traffic_data = cleansed_data.select(
                    func.col("event_date") ,
                    func.hour(func.col("event_time")).alias("hour"),
                    func.col("is_bot")
                    )

# traffic_data.show()

traffic_table = traffic_data.groupBy(
                                func.col("event_date"), 
                                func.col("hour"), func.col("is_bot")
                                ).count().withColumnRenamed("count","requests_count")





requests_data = cleansed_data.select(

        func.col("event_date"),
        func.hour(func.col("event_time")).alias("hour"),
        func.col("method")
)


requests_table = requests_data.groupBy(

                func.col("event_date"),
                func.col("hour"),
                func.col("method")
            ).count().withColumnRenamed("count" , "requests_count")



# Store Traffic table

traffic_table.write\
        .mode("overwrite")\
        .format("parquet")\
        .save(f"hdfs://localhost:9000/{TRAFFIC_TABLE_PATH}")


# Store Requsts Table

requests_table.write\
        .mode("overwrite")\
        .format("parquet")\
        .save(f"hdfs://localhost:9000/{REQUESTS_TABLE_PATH}")



