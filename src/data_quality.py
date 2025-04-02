from pyspark.sql import DataFrame
from pyspark.sql.functions import col

class DataQuality:
    def __init__(self, df: DataFrame):
        self.df = df
    
    def schema(self):
        return self.df.printSchema()