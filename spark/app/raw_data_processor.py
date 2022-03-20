import sys
from pyspark.sql import SparkSession
from modules.ratings_reader import RatingsReader
from modules.metadata_reader import MetadataReader

# Create spark session
spark = SparkSession \
    .builder \
    .getOrCreate()

####################################
# Parameters
####################################
# movies_file = sys.argv[1]
# ratings_file = sys.argv[2]

table_name = sys.argv[1]
target_folder = sys.argv[2]
obj_type = sys.argv[3]
postgres_db = sys.argv[4]
postgres_user = sys.argv[5]
postgres_pwd = sys.argv[6]

####################################
# Read raw Data
####################################
if obj_type == 'ratings':
    data_reader = RatingsReader()
elif obj_type == 'metadata':
    data_reader = MetadataReader()
else:
    raise ValueError(f'Unexpected obj_type {obj_type}')
    
movies_tv_df = data_reader.get_df_from_file(spark, target_folder)


####################################
# Load data to Postgres
####################################
print("######################################")
print("LOADING POSTGRES TABLES")
print("######################################")


movies_tv_df.write \
    .format("jdbc") \
    .option("url", postgres_db) \
    .option("dbtable", table_name) \
    .option("user", postgres_user) \
    .option("password", postgres_pwd) \
    .mode("overwrite") \
    .save()
