from modules.data_reader import DataReader
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql import SparkSession

class MetadataReader(DataReader):
    def __init__(self):
        self.schema = StructType([
            StructField('asin', StringType(), True),
            StructField('categories', StringType(), True),
            StructField('description', StringType(), True),
            StructField('title', StringType(), True),
            StructField('salesRank', StringType(), True),
            StructField('imUrl', StringType(), True),
            StructField('brand', StringType(), True),
            StructField('related', StringType(), True),
            StructField('price', FloatType(), True)])

        self.key_cols = ['asin']

    def get_df_from_file(self, spark: SparkSession, target_folder: str):
        metadata_df = spark \
            .read \
            .option('mode', 'DROPMALFORMED') \
            .json(f'{target_folder}/*.gz', schema=self.schema, allowBackslashEscapingAnyCharacter=True)
        metadata_df = metadata_df \
            .withColumnRenamed('salesRank', 'sales_rank') \
            .withColumnRenamed('imUrl', 'im_url')

        metadata_df = metadata_df.dropDuplicates(self.key_cols) \
            .repartition(*self.key_cols)
        return metadata_df