from pyspark.sql import SparkSession
from extract import DataExtractor
from data_quality import DataQuality


def run_pipeline():
    spark = SparkSession.builder.appName("Movie_Analytics_ETL").getOrCreate()

    extractor = DataExtractor(spark)

    # Extract data from the source
    df_movies = extractor.extract_data("../project_data/movies_main.csv", "csv", header=True, inferSchema=True)
    df_extended = extractor.extract_data("../project_data/movie_extended.csv", "csv", header=True, inferSchema=True)
    df_ratings = extractor.extract_data("../project_data/ratings.json", "json")

    dq_df_movies = DataQuality(df_movies)
    dq_df_movies.schema()
    df_movies.show(5)
    
    spark.stop()


if __name__ == "__main__":
        run_pipeline()