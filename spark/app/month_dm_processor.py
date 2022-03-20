import sys
from pyspark.sql import SparkSession
from modules.pg_service import PGService
from modules.ratings_reader import RatingsReader
from modules.metadata_reader import MetadataReader
from pyspark.sql.functions import date_format, avg
from utils.logging import get_logger

# Create spark session
spark = SparkSession \
    .builder \
    .getOrCreate()

sc = spark.sparkContext
logger = get_logger(sc.appName)
try:
    ####################################
    # Parameters
    ####################################

    metadata_table_name = sys.argv[1]
    ratings_table_name = sys.argv[2]
    month_dm_table_name = sys.argv[3]
    postgres_db = sys.argv[4]
    postgres_user = sys.argv[5]
    postgres_pwd = sys.argv[6]

    ####################################
    # Agg existing Data
    ####################################

    # Init pg connecttion
    pgs = PGService(postgres_db, postgres_user, postgres_pwd)
    month_col = 'month'

    metadata_df = pgs.load_df_from_table(spark, metadata_table_name)
    logger.debug(f'metadata_df count: {metadata_df.count()}')
    
    ratings_df = pgs.load_df_from_table(spark, ratings_table_name)
    logger.debug(f'ratings_df count: {ratings_df.count()}')

    ratings_df = ratings_df.join(
        metadata_df,
        metadata_df['asin']==ratings_df['item'],
        how='inner'
    )
    ratings_df = ratings_df \
        .withColumn(month_col,  date_format(ratings_df['ts'], 'yyyy-MM'))

    ratings_df = ratings_df \
        .groupBy(month_col, 'item', 'title', 'brand', 'price') \
        .agg(avg('rating').alias('avg_rating'))
        

    ####################################
    # Load data to Postgres
    ####################################

    pgs.save_df_to_table(ratings_df, month_dm_table_name, 'overwrite')
except Exception as e:
    logger.error(f'Failed processing raw data with args {sys.argv}:\n{e}')
