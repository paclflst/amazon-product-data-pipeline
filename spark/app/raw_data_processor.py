import sys
from pyspark.sql import SparkSession
from modules.pg_service import PGService
from modules.ratings_reader import RatingsReader
from modules.metadata_reader import MetadataReader

# Create spark session
spark = SparkSession \
    .builder \
    .getOrCreate()

####################################
# Parameters
####################################

table_name = sys.argv[1]
target_folder = sys.argv[2]
obj_type = sys.argv[3]
postgres_db = sys.argv[4]
postgres_user = sys.argv[5]
postgres_pwd = sys.argv[6]

####################################
# Read raw Data
####################################

# Init pg connecttion
pgs = PGService(postgres_db, postgres_user, postgres_pwd)

if obj_type == 'ratings':
    data_reader = RatingsReader()
elif obj_type == 'metadata':
    data_reader = MetadataReader()
else:
    raise ValueError(f'Unexpected obj_type {obj_type}')
on_cols = data_reader.key_cols
    
source_df = data_reader.get_df_from_file(spark, target_folder)

####################################
# Load data to Postgres
####################################
print("######################################")
print("LOADING POSTGRES TABLES")
print("######################################")

pgs.save_df_new_rows_to_table(spark, source_df, table_name, on_cols)
