from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql import functions as func
from pyspark.sql.functions import expr
from datetime import date,timedelta
import re
import sys
import subprocess
RUN_DATE = date.today()
PARENT_DIR = f"date={RUN_DATE}"
ERROR_SILVER_PATH ="/user/linux/silver/error"
TRANSFORMATION_SILVER_PATH ="/user/linux/silver/transformations"
BRONZE_PATH = "/user/linux/bronze"
FILE_NAME = 'access_logs.txt'

spark = SparkSession.builder.appName("Log Analysis Pipeline").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")



def mapping(line):

    try:
        log_pattern = re.compile(
            r'(?P<ip>\S+) \S+ \S+ \[(?P<request_ddtm>.+?)\] '
            r'"(?P<method>\S+) (?P<path>\S+) (?P<protocol>\S+)" '
            r'(?P<status>\d+) (?P<size>\S+) '
            r'"(?P<referer>.*?)" "(?P<agent>.*?)" "(?P<extra>.*?)"'
        )

        dict_result = log_pattern.match(line).groupdict()
    

        return ("VALID" ,  Row(ip=dict_result["ip"] , event_dttm=dict_result["request_ddtm"] , method = dict_result["method"], path = dict_result['path'],
                protocol=dict_result['protocol'], status = dict_result['status'], size= dict_result['size'], referer = dict_result['referer'] , agent=dict_result['agent'],
                extra=dict_result['extra']
                ))

    except AttributeError :

        return ("INVALID" , line)






rdd_raw_logs = spark.sparkContext.binaryFiles(f"hdfs://localhost:9000/{BRONZE_PATH}/{PARENT_DIR}/{FILE_NAME}").flatMap(lambda x: x[1].decode("utf-8", errors='ignore').splitlines())


rdd_flatten_logs = rdd_raw_logs.map(mapping)


error_records = rdd_flatten_logs.filter(lambda x : x[0] == 'INVALID')
valid_records =rdd_flatten_logs.filter(lambda x : x[0] == 'VALID')




error_schema = StructType([StructField("category" , StringType() , False) , StructField("line" , StringType() , False)])
error_df = spark.createDataFrame(error_records , schema = error_schema)


xx = error_df.count()

print("===============================================================")
print("COUNT OF VALID: " , valid_records.count())
print("===============================================================")


print("===============================================================")
print("COUNT: " , xx)
print("===============================================================")

# logs_schema = StructType([

#     StructField("ip" , StringType() , False),
#     StructField("ip" , StringType() , False),
#     StructField("ip" , StringType() , False),
#     StructField("ip" , StringType() , False),
#     StructField("ip" , StringType() , False),
#     StructField("ip" , StringType() , False),

# ])



logs_data_rdd = valid_records.map(lambda x: x[1])



logs_df = spark.createDataFrame(logs_data_rdd)




# Add Date Only Column , Time Only Column , IS Bot




refine_df = logs_df.withColumn('event_date' , func.to_date(
        func.to_timestamp(func.col("event_dttm"), "dd/MMM/yyyy:HH:mm:ss Z")
    )  )\
    .withColumn('event_time' , func.date_format(
        func.to_timestamp(func.col("event_dttm"), "dd/MMM/yyyy:HH:mm:ss Z"), 
        "HH:mm:ss"
    ))\
    .withColumn('is_bot' , func.when(
            func.col("agent").rlike("(?i)(bot|crawler|spider|scraper|slurp|curl|wget|python|java)") | func.col("agent").isNull() , 
            'bot'

    ).otherwise('human'))






# Drop non-need columns
cleansed_log_df = refine_df.select(

                              func.col('ip'),
                              func.col('event_date'),
                              func.col('event_time'),
                              func.col('method'),
                              func.col('status'),
                              func.col('is_bot')

                              )


# Create Date DIR In Error DIR
try:
    result = subprocess.run(['hdfs','dfs','-mkdir' , '-p',f'{ERROR_SILVER_PATH}/{PARENT_DIR}'] , check=True , capture_output=True)
except subprocess.CalledProcessError:
    print(f"ERROR: Can't Create this DIR {ERROR_SILVER_PATH/PARENT_DIR}")
    sys.exit(1)

# Create Date DIR In Transformations DIR
try:
    result = subprocess.run(['hdfs','dfs','-mkdir' , '-p',f'{TRANSFORMATION_SILVER_PATH}/{PARENT_DIR}'] , check=True , capture_output=True)
except subprocess.CalledProcessError:
    print(f"ERROR: Can't Create this DIR {ERROR_SILVER_PATH/PARENT_DIR}")
    sys.exit(1)

# Store errors to error path
error_df.write \
    .mode('overwrite')\
    .format("parquet") \
    .option("path", f"hdfs://localhost:9000/{ERROR_SILVER_PATH}/{PARENT_DIR}") \
    .saveAsTable("error_table")



cleansed_log_df.write\
               .mode('overwrite')\
               .format('parquet')\
               .save(f"hdfs://localhost:9000/{TRANSFORMATION_SILVER_PATH}/{PARENT_DIR}")               


