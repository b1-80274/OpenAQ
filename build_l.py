def build_latest():
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import *

    # creating a spark session
    spark = SparkSession.builder \
        .appName("warehouse_dump") \
        .getOrCreate()

    # build the latest table
    schema="locationId INT, utc_time TIMESTAMP, parameter STRING, value DOUBLE"
    wrh_data = spark.read\
        .option('schema',schema)\
        .orc('hdfs://localhost:9000/user/OpenAQ/data/input')

    recent_df = wrh_data\
        .filter(year('local').between(year(current_date()) - 1, year(current_date())))

    # Dump to HDFS
    recent_df.write\
        .option('header',True)\
        .format("csv").mode('overwrite')\
        .save("hdfs://localhost:9000/user/OpenAQ/data/latest")

    # save to Local FS
    recent_df.write\
        .option('header',True)\
        .format("csv").mode('overwrite')\
        .save("/home/sad7_5407/Downloads/OpenAQ/latest")

build_latest()
