import credentials as cr
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, StringType, broadcast, coalesce, when

import pygeohash
import requests

api_opencage = cr.api_opencage
client_secret = cr.client_secret
hotels_file_path = cr.hotels_file_path
weather_file_path = cr.weather_file_path
azure_write_path = cr.azure_write_path
azure_client_id = cr.azure_client_id
jar_packages = cr.jar_packages
storage_account_name = cr.storage_account_name
microsoft_login = cr.microsoft_login


class SparkInitializer:

    def __init__(self, app_name, jar_packages):
        self.app_name = app_name
        self.jar_packages = jar_packages

    def create_spark_session(self):
        self.spark = SparkSession.builder.appName(self.app_name).config('spark.jars.packages', self.jar_packages).getOrCreate()
        self.spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "OAuth")
        self.spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net",
                            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
        self.spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net",
                            azure_client_id)
        self.spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net",
                            client_secret)
        self.spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net",
                            microsoft_login)
        return self.spark


class Extract:
    def __init__(self, spark):
        self.spark = spark

    def read_csv_data(self, file_path):
        csv_data = self.spark.read.csv(file_path, header=True, inferSchema=True)
        return csv_data

    def read_parquet_data(self, file_path):
        parquet_data = self.spark.read.parquet(file_path, header=True, inferSchema=True)
        return parquet_data


class Transform:

    def hotels_null_handling(self, hotels):

        get_latitude_udf = udf(self.get_latitude)
        get_longitude_udf = udf(self.get_longitude)

        # create new columns with correct values using OpenCage Geocoding API

        hotels = hotels.withColumn("CorrectLatitude", when((col("Latitude").isNull()) | (col("Longitude").isNull()),
                                                           get_latitude_udf(col("Address"), col("City"),
                                                                            col("Country"))))
        hotels = hotels.withColumn("CorrectLongitude", when((col("Latitude").isNull()) | (col("Longitude").isNull()),
                                                            get_longitude_udf(col("Address"), col("City"),
                                                                              col("Country"))))

        # replace null values with correct values
        hotels = hotels.withColumn("Latitude", coalesce(col("Latitude"), col("CorrectLatitude")))
        hotels = hotels.withColumn("Longitude", coalesce(col("Longitude"), col("CorrectLongitude")))

        # drop temporary columns
        hotels = hotels.drop("CorrectLatitude", "CorrectLongitude")

        return hotels

    def hotels_add_geohashing(self, hotels_data):
        # add geohash column
        encode_geohash = udf(lambda lat, lon: pygeohash.encode(lat, lon, precision=4), StringType())
        hotels_data = hotels_data.withColumn('Geohash', encode_geohash(col('Latitude'), col('Longitude')))

        return hotels_data

    def weather_add_geohashing(self, weather_data):
        # add geohash column to weather dataframe
        encode_geohash = udf(lambda lat, lon: pygeohash.encode(lat, lon, precision=4), StringType())
        weather_data = weather_data.withColumn('Geohash', encode_geohash(col('lat'), col('lng')))

        return weather_data

    def join_hotels_weather_data(self, hotels, weather):

        # join hotels and weather dataframes on geohash column
        joined_data = hotels.join(broadcast(weather), 'Geohash', 'left')

        return joined_data

    def get_latitude(self, address, city, country):
        api_url = f'https://api.opencagedata.com/geocode/v1/json?q={address}, {city}, {country}&key={api_opencage}'
        response = requests.get(api_url)
        if response.status_code == 200:
            results = response.json()['results']
            if len(results) > 0:
                return results[0]['geometry']['lat']

    def get_longitude(self, address, city, country):
        api_url = f'https://api.opencagedata.com/geocode/v1/json?q={address}, {city}, {country}&key={api_opencage}'
        response = requests.get(api_url)
        if response.status_code == 200:
            results = response.json()['results']
            if len(results) > 0:
                return results[0]['geometry']['lng']


class Load:
    def write_to_azure_storage(self, data, file_path):

        data.write.format('parquet').mode('overwrite').option('compression', 'snappy') \
            .partitionBy('year', 'month', 'day').option('path', file_path)


if __name__ == "__main__":
    # set up Spark ETL instance
    spark = SparkInitializer(
        app_name="Spark-ETL",
        jar_packages=cr.jar_packages)

    spark_session = spark.create_spark_session()

    extractor = Extract(spark_session)
    transformer = Transform()
    loader = Load()

    hotels = extractor.read_csv_data(hotels_file_path)
    weather = extractor.read_parquet_data(weather_file_path)

    hotels = transformer.hotels_null_handling(hotels)

    hotels = transformer.hotels_add_geohashing(hotels)
    weather = transformer.weather_add_geohashing(weather)

    merged_data = transformer.join_hotels_weather_data(hotels, weather)
    loader.write_to_azure_storage(merged_data, azure_write_path)
    print(merged_data)
    merged_data.show()