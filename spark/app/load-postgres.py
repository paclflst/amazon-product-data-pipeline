import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, col, to_timestamp
from pyspark.sql.types import DoubleType, StructType, StructField, IntegerType, StringType, FloatType, LongType

# Create spark session
spark = (SparkSession
    .builder
    .getOrCreate()
)

####################################
# Parameters
####################################
movies_file = sys.argv[1]
ratings_file = sys.argv[2]
postgres_db = sys.argv[3]
postgres_user = sys.argv[4]
postgres_pwd = sys.argv[5]

####################################
# Read CSV Data
####################################
print("######################################")
print("READING CSV FILES NEW 51")
print(postgres_db)
print(postgres_user)
print(postgres_pwd)
print(movies_file)
print(ratings_file)
print("######################################")



df_movies_csv = spark.read.option("header", True).csv(movies_file)
print(df_movies_csv.head(10))
df_movies_csv.head(10)
print('cnt', df_movies_csv.count())
print("######################################")
print("FIRST CSV FILE")


df_ratings_csv = (
    spark.read
    .format("csv")
    .option("header", True)
    .load(ratings_file)
    .withColumnRenamed("timestamp","timestamp_epoch")
)

# Convert epoch to timestamp and rating to DoubleType
df_ratings_csv_fmt = (
    df_ratings_csv
    .withColumn('rating', col("rating").cast(DoubleType()))
    .withColumn('timestamp', to_timestamp(from_unixtime(col("timestamp_epoch"))))
)

####################################
# Load data to Postgres
####################################
print("######################################")
print("LOADING POSTGRES TABLES")
print("######################################")

df_movies_csv.write \
    .format("jdbc") \
    .option("url", postgres_db) \
    .option("dbtable", "public.movies") \
    .option("user", postgres_user) \
    .option("password", postgres_pwd) \
    .mode("overwrite") \
    .save() 

(
     df_ratings_csv_fmt
     .select([c for c in df_ratings_csv_fmt.columns if c != "timestamp_epoch"])
     .write
     .format("jdbc")
     .option("url", postgres_db)
     .option("dbtable", "public.ratings")
     .option("user", postgres_user)
     .option("password", postgres_pwd)
     .mode("overwrite")
     .save()
)
print("######################################")
print("LOADING BIG TABLES")
print("######################################")

schema = StructType([
    StructField("user", StringType(), True),
    StructField("item", StringType(), True),
    StructField("rating", FloatType(), True),
    StructField("timestamp", LongType(), True)])

df_movies_tv_df = spark.read.csv('/usr/local/spark/resources/data/ratings_Movies_and_TV.csv', header=False,schema=schema) 
df_movies_tv_df.write \
    .format("jdbc") \
    .option("url", postgres_db) \
    .option("dbtable", "public.ratings_movies_and_tv") \
    .option("user", postgres_user) \
    .option("password", postgres_pwd) \
    .mode("overwrite") \
    .save() 