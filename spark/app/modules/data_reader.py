import abc
from pyspark.sql import SparkSession

class DataReader(object):
    __metaclass__ = abc.ABCMeta
    
    @abc.abstractmethod
    def get_df_from_file(self, spark: SparkSession, target_folder: str):
        """Method documentation"""
        return df