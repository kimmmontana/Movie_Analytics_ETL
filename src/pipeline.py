from pyspark.sql import SparkSession
from extract import DataExtractor
from data_quality import DataQuality
from transform_bronze import MovieBronzeLayer, MovieExtendedBronzeLayer, RatingsBronzeLayer
#from transform_silver import MovieSilverLayer, MovieExtendedSilverLayer, RatingsSilverLayer

def run_pipeline():
    '''
    This is the main function that runs the ETL pipeline.
    '''

    #Initialization
    spark = SparkSession.builder.appName("Movie_Analytics_ETL").getOrCreate()

    #Extract
    Extractor = DataExtractor(spark)
    sourceDataframes = Extractor.get_all_dataframes()

    # #Data Quality Check of Source Dataframes
    # for key in sourceDataframes.keys():
    #     DataQuality(sourceDataframes[key], key)

    # bronzeMovieDf = MovieBronzeLayer(sourceDataframes['movies'], 'movies')
    # bronzeMovieDf.transform_df()

    # bronzeMovieExtendedDf = MovieExtendedBronzeLayer(sourceDataframes['extended'], 'extended')
    # bronzeMovieExtendedDf.transform_df()

    bronzeRatingsDF = RatingsBronzeLayer(sourceDataframes['ratings'], 'ratings')
    bronzeRatingsDF.transform_df()

    #Silver Layer
    # bronzeMovieDf = MovieBronzeLayer(sourceDataframes['movies'], 'movies')
    # bronzeMovieDf.transform_df()
    # bronzeMovieExtendedDf = MovieExtendedBronzeLayer(sourceDataframes['extended'], 'extended')
    # bronzeMovieExtendedDf.transform_df()
    # bronzeRatingsDF = RatingsBronzeLayer(sourceDataframes['ratings'], 'ratings')
    # bronzeRatingsDF.transform_df()

    # #Data Quality Check of Bronze Dataframes
    # for key in sourceDataframes.keys():   
    #     DataQuality(sourceDataframes[key], key)
    #     DataQuality(bronzeMovieDf, 'movies')
    #     DataQuality(bronzeMovieExtendedDf, 'extended')
    #     DataQuality(bronzeRatingsDF, 'ratings')
    #     DataQuality(bronzeMovieExtendedDf, 'extended')
    #     DataQuality(bronzeRatingsDF, 'ratings')
    #     DataQuality(bronzeRatingsDF, 'ratings')
    #     DataQuality(bronzeMovieExtendedDf, 'extended')
    #     DataQuality(bronzeRatingsDF, 'ratings')
    #     DataQuality(bronzeMovieExtendedDf, 'extended')
    #     DataQuality(bronzeRatingsDF, 'ratings')
    #     DataQuality(bronzeMovieExtendedDf, 'extended')
    #     DataQuality(bronzeRatingsDF, 'ratings')
    #     DataQuality(bronzeMovieExtendedDf, 'extended')
    #     DataQuality(bronzeRatingsDF, 'ratings')

    spark.stop()


if __name__ == "__main__":
        run_pipeline()