def etl_process():
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import *
    from pyspark.sql.utils import AnalysisException
    import time
    import os
    import json
    import requests
    import pandas as pd
    from datetime import datetime

    location_parameters = {
        67569: ['um050', 'pressure', 'humidity', 'temperature', 'um005', 'um003', 'pm1', 'um025', 'um010', 'um100', 'pm25',
                'pm10'],  # Bangalore
        288233: ["pm25", "um100", "pressure", "um010", "um025", "pm10", "um003", "humidity", "um005", "pm1", "temperature",
                 "um050"],  # Kalol, Gandhi Nagar
        64931: ["um100", "um050", "um005", 'pm10', 'um025', 'pm1', 'pm25', 'um003', 'um010'],  # Bhatinda
        66673: ['um003', 'um003', 'pm10', 'um005', 'pm25', 'um010''um100''pressure', 'um050', 'um025', 'pm1', 'temperature',
                'humidity'],  # Hisar
        8118: ["pm25"],  # New Delhi
        62543: ['pressure', 'pm25', 'um010', 'humidity', 'um003', 'temperature', 'um100', 'um025', 'um050', 'um005', 'pm10',
                'pm1'],  # Greater Kailash 2
        1667903: ['um010', 'humidity', 'temperature', 'um050', 'pm1', 'um003', 'um005', 'pm25', 'pm10', 'um025', 'pressure',
                  'um100'],  # 15 Oak Drive Outdoor
        362098: ['temperature', 'um050', 'pressure', 'um003', 'um005', 'humidity', 'um025', 'um100', 'um010', 'pm10', 'pm1',
                 'pm25'],  # Greater Noida
        67569: ['pm10', 'um010', 'um100', 'um050', 'um005', 'pressure', 'um025', 'um003', 'temperature', 'pm1', 'humidity',
                'pm25'],  # Tarkeshwar, West Bengal
        8172: ['pm25'],  # Kolkata
        220704: ['temperature', 'um010', 'pm10', 'pm25', 'pressure', 'um025', 'humidity', 'temperature', 'um003', 'um100',
                 'um050', 'um005', 'pm1'],  # Kharagpur, West Bengal
        8039: ['pm25'],  # Mumbai
        8557: ['pm25'],  # Hyderabad
        63704: ['um003', 'um025', 'pm10', 'pm1', 'humidity', 'temperature', 'um010', 'um005', 'um050', 'pm25', 'um100',
                'pressure'],  # Madikeri, Karnataka
        229138: ['um050', 'pm10', 'temperature', 'humidity', 'pm1', 'um003', 'pressure', 'um025', 'um005', 'um010', 'um100',
                 'pm25'],  # Srinivaspur, Karnataka
        8558: ['pm25']  # Chennai
    }

    govt_sensors = set([8039, 8118, 8172, 8557, 8558])

    # creating a spark session
    spark = SparkSession.builder\
        .appName("warehouse_dump")\
        .getOrCreate()

    # get the warehouse table
    schema="locationId INT, local_time TIMESTAMP, parameter STRING, value DOUBLE"
    df = spark.read\
        .option('schema',schema)\
        .orc('hdfs://localhost:9000/user/OpenAQ/data/input')

    # get the max date
    start_date = df.agg(max(col('utc')).cast('date')).collect()[0][0]

    # get the today's date
    today_datetime = datetime.fromtimestamp(time.time())
    today_date = today_datetime.date()

    date_range = pd.date_range(start=start_date, end=today_date, freq='D')

    def save_data():
        if location_id in govt_sensors:
            # govt
            url = f"https://api.openaq.org/v2/measurements?location_id={location_id}&parameter={parameter}&date_from={date_from}T00:00:00+05:30&date_to={date_to}T00:00:00+05:30"
        else:
            # community
            url = f"https://api.openaq.org/v2/measurements?location_id={location_id}&parameter={parameter}&date_from={date_from}T21:00:00+05:30&date_to={date_to}T00:00:00+05:30"
        print(url)
        response = requests.get(url)
        if response.status_code == 200:
            print('Hit Success.....')
            # Access the response data in JSON format
            data = response.json()
            # check if you've got some data or not
            if len(data['results']) > 0:
                with open(f'/home/sad7_5407/Desktop/Data Engineering/data/{yr}/{mnt}/{dy}/{location_id}/{parameter}.json', 'w') as file:
                    json.dump(data['results'], file, indent=2)
            else:
                print(f'>>>>>>Nothing on {url}')
        else:
            print(f">>>>>>Error in govt hit: {response.status_code}")
        time.sleep(2)


    for i in range(len(date_range)-1):
        date_from = str(date_range[i])[:10]
        date_to = str(date_range[i+1])[:10]

        dy = date_from[-2:]
        mnt = date_from[5:7]
        yr = date_from[:4]

        for location_id in location_parameters.keys():
            # make the directory of location_id
            os.makedirs(f'data/{yr}/{mnt}/{dy}/{location_id}', exist_ok=True)
            for parameter in location_parameters[location_id]:
                save_data()



    # reading the local data
    for i in range(len(date_range)-1):
        for location_id in location_parameters.keys():
            date_from = str(date_range[i])[:10]
            date_to = str(date_range[i + 1])[:10]

            dy = date_from[-2:]
            mnt = date_from[5:7]
            yr = date_from[:4]

            for parameter in location_parameters[location_id]:
                try:
                    data = spark.read \
                        .option('multiline', True) \
                        .json(f'data/{yr}/{mnt}/{dy}/{location_id}/{parameter}.json')

                    # selecting the required columns
                    final_df = data.select('locationId', 'date.utc', 'parameter', 'value')

                    # dumping into warehouse
                    final_df.write.format("orc") \
                        .mode('append').save("hdfs://localhost:9000/user/OpenAQ/data/input")

                except AnalysisException:
                    print(f'inferSchema failed for {location_id}/{parameter}..')


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

etl_process()
