from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, LongType
from pyspark.sql.functions import col, when, lit
import json

class DataExtractor:
    def __init__(self, spark: SparkSession):
        """
        Initializes the DataExtractor with a Spark session.
        """
        self.spark = spark


    def extract_movies(self):
        return self.spark.read.csv("../project_data/movies_main.csv", header=True, inferSchema=True)
 
    def extract_extended(self):
        return self.spark.read.csv("../project_data/movie_extended.csv", header=True, inferSchema=True)
    
    def extract_ratings(self):
        with open('../project_data/ratings.json', 'r') as file:
            data = json.load(file)

        # Replace NaN with null (None) in the data
        for record in data:
            if 'ratings_summary' in record:
                ratings_summary = record['ratings_summary']
                # Replace 'NaN' or invalid value with None (null)
                if ratings_summary.get('std_dev') == 'NaN' or ratings_summary.get('std_dev') is None:
                    ratings_summary['std_dev'] = None


        with open('../project_data/ratings_cleaned.json', 'w') as file:
            json.dump(data, file)

        # Load the cleaned JSON file into a Spark DataFrame
        ratings_df = self.spark.read.json('../project_data/ratings_cleaned.json')

        ratings_df_cleaned = ratings_df.select(
            "movie_id",
            "ratings_summary.avg_rating",
            "ratings_summary.total_ratings",
            "ratings_summary.std_dev",
            "last_rated"
        )
        return ratings_df_cleaned
    
    def get_all_dataframes(self):

        return {
            "movies": self.extract_movies(),
            "extended": self.extract_extended(),
            "ratings": self.extract_ratings()
        }