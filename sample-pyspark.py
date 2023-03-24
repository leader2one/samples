import time
import pandas
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, udf, StringType, broadcast
import pygeohash
import requests
api_opencage = "fd6afbc92119441386ea330fffa07ffc"

class SparkETL:

    def __init__(self, app_name, jar_packages, service_credential):
        self.spark = SparkSession.builder.appName(app_name).config('spark.jars.packages', jar_packages).getOrCreate()
        self.spark.conf.set("fs.azure.account.auth.***.dfs.core.windows.net", "OAuth")
        self.spark.conf.set("fs.azure.account.oauth.provider.type.****.dfs.core.windows.net",
                       "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
        self.spark.conf.set("fs.azure.account.oauth2.client.id.***.dfs.core.windows.net",
                       "f3905ff9-16d4-43ac-9011-842b661d556d")
        self.spark.conf.set("fs.azure.account.oauth2.client.secret.****.dfs.core.windows.net", service_credential)
        self.spark.conf.set("fs.azure.account.oauth2.client.endpoint.****.dfs.core.windows.net",
                       "https://login.microsoftonline.com/***/token")

    def read_hotels_data(self, file_path):
        hotels = self.spark.read.csv(file_path, header=True, inferSchema=True)

        # map incorrect (null) values to correct values using OpenCage Geocoding API
        for row in hotels.rdd.collect():
            if row.Latitude is None or row.Longitude is None:
                # call OpenCage Geocoding API to get correct values
                api_url = f'https://api.opencagedata.com/geocode/v1/json?q={row.Address}, {row.City}, {row.Country}&key={api_opencage}'
                response = requests.get(api_url)
                if response.status_code == 200:
                    results = response.json()['results']
                    if len(results) > 0:
                        lat = results[0]['geometry']['lat']
                        lon = results[0]['geometry']['lng']
                        # update dataframe with correct values
                        hotels = hotels.withColumn('Latitude',
                                                   when(col('Id') == row.Id, lat).otherwise(col('Latitude')))
                        hotels = hotels.withColumn('Longitude',
                                                   when(col('Id') == row.Id, lon).otherwise(col('Longitude')))

        # add geohash column
        encode_geohash = udf(lambda lat, lon: pygeohash.encode(lat, lon, precision=4), StringType())
        hotels = hotels.withColumn('Geohash', encode_geohash(col('Latitude'), col('Longitude')))
        #hotels = hotels.withColumn('Geohash', pygeohash.encode(col('Latitude'), col('Longitude'), precision=4))
        return hotels

    def join_weather_data(self, hotels_df):
        file_path = "abfss://***/"
        weather = self.spark.read.parquet(file_path, header=True, inferSchema=True)
        print(weather)
        #weather.show()

        # add geohash column to weather dataframe
        encode_geohash = udf(lambda lat, lon: pygeohash.encode(lat, lon, precision=4), StringType())
        weather = weather.withColumn('Geohash', encode_geohash(col('lat'), col('lng')))

        # join hotels and weather dataframes on geohash column
        joined = hotels_df.join(broadcast(weather), 'Geohash', 'left')

        return joined
    def write_to_azure_storage(self, df, file_path):
        # write to Azure ADLS Gen2 storage in parquet format with partition
        df.write.format('parquet').mode('overwrite').option('compression', 'snappy') \
            .partitionBy('year', 'month', 'day').option('path', file_path)

if __name__ == "__main__":
    # set up Spark ETL instance
    spark_etl = SparkETL(
        app_name="Spark-ETL",
        jar_packages="org.apache.hadoop:hadoop-azure:3.3.0",
        service_credential="AkI8Q~AocBjw2~R3rMi-b-2VIlsFzNTiD6kTGcv7")


    hotels = spark_etl.read_hotels_data("abfss://****")
    print('done', hotels)
    print(type(hotels))
    joined = spark_etl.join_weather_data(hotels)
    print(joined)
    #hotels.show()
    #joined.show()
    write_path = "abfss://****/output"
    spark_etl.write_to_azure_storage(joined, write_path)
    print('writtent')
