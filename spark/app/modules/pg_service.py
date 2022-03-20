from pyspark.sql import SparkSession, DataFrame

class PGService:
    def __init__(self, pg_url: str, pg_user: str, pg_password:str):
        self.pg_url = pg_url
        self.pg_user = pg_user
        self.pg_password = pg_password

    def save_df_to_table(self, df: DataFrame, pg_table: str, mode: str = 'append', batch: int = 100000):
        df.write \
            .format("jdbc") \
            .option("url", self.pg_url) \
            .option("dbtable", pg_table) \
            .option("user", self.pg_user) \
            .option("password", self.pg_password) \
            .option("batchsize", batch) \
            .mode(mode) \
            .save()
        return True

    def load_df_from_table(self, spark: SparkSession, pg_table: str) -> DataFrame:
        return spark.read \
            .format("jdbc") \
            .option("url", self.pg_url) \
            .option("dbtable", pg_table) \
            .option("user", self.pg_user) \
            .option("password", self.pg_password) \
            .load()

    def save_df_new_rows_to_table(self, spark: SparkSession, df: DataFrame, pg_table: str, on_cols: [str]):
        existing_df = self.load_df_from_table(spark, pg_table)
        existing_df = existing_df.repartition(*on_cols)

        df = df.join(
            existing_df,
            on_cols,
            how='left_anti'
        )
        self.save_df_to_table(df, pg_table, 'append')