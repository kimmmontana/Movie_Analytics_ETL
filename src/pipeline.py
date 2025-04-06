from pyspark.sql import SparkSession
from extract import DataExtractor
from data_quality import DataQuality
from transform_bronze import MovieBronzeLayer, MovieExtendedBronzeLayer, RatingsBronzeLayer


def run_pipeline():
    #This is the main function that runs the ETL pipeline.

    #Initialization
    spark = SparkSession.builder.appName("Movie_Analytics_ETL").getOrCreate()

    #Extract
    Extractor = DataExtractor(spark)
    sourceDataframes = Extractor.get_all_dataframes()

    # #Data Quality Check of Source Dataframes
    # for key in sourceDataframes.keys():
    #     DataQuality(sourceDataframes[key], key)

    test1 = MovieBronzeLayer(sourceDataframes['movies'], 'movies')
    test1.test_transform_df()

    # #Transformation to Bronze Layer
    # for key in sourceDataframes.keys():
    #     if key.lower() == "movies":
    #         MovieBronzeLayer(sourceDataframes[key], key)
    #     elif key.lower() == "extended":
    #         MovieExtendedBronzeLayer(sourceDataframes[key], key)
    #     elif key.lower() == "ratings":
    #         RatingsBronzeLayer(sourceDataframes[key], key)
    #     else:
    #         raise ValueError(f"No BronzeLayer defined for source: {key}")

    spark.stop()


if __name__ == "__main__":
        run_pipeline()