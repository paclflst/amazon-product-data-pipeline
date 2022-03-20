from modules.data_reader import DataReader
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime

class RatingsReader(DataReader):
    def __init__(self):
        self.schema = StructType([
            StructField("user", StringType(), True),
            StructField("item", StringType(), True),
            StructField("rating", FloatType(), True),
            StructField("ts", LongType(), True)])

    def get_df_from_file(self, spark: SparkSession, target_folder: str):
        ratings_df = spark \
            .read \
            .csv(f'{target_folder}/*.csv', header=False, schema=self.schema)
        ratings_df = ratings_df \
            .withColumn('ts', from_unixtime(ratings_df['ts']))
        return ratings_df