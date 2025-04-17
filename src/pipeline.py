from pyspark.sql import SparkSession
from extract import DataExtractor
from data_quality import DataQuality
from transform_bronze import MovieBronzeLayer, MovieExtendedBronzeLayer, RatingsBronzeLayer
from transform_silver import SilverLayer
from load import Load

def run_pipeline():
    '''
    This is the main function that runs the ETL pipeline.
    '''

    #Initialization
    spark = SparkSession.builder.appName("Movie_Analytics_ETL").config("spark.jars", "mysql-connector-j-8.0.33.jar").getOrCreate()
    print(spark.sparkContext._jsc.sc().listJars())

    #Extract
    Extractor = DataExtractor(spark)
    sourceDataframes = Extractor.get_all_dataframes()


    # Bronze Layer Transformation
    bronzeMovieDF = MovieBronzeLayer(sourceDataframes['movies'], 'movies')
    bronzeMovieDF.transform_df()
    bronzeMovieExtendedDF = MovieExtendedBronzeLayer(sourceDataframes['extended'], 'extended')
    bronzeMovieExtendedDF.transform_df()
    bronzeRatingsDF = RatingsBronzeLayer(sourceDataframes['ratings'], 'ratings')
    bronzeRatingsDF.transform_df()

    #Silver Layer Transformation
    OLAPDataFrames = SilverLayer(bronzeMovieDF.bronzeMovieDF, 
                        bronzeMovieExtendedDF.bronzeMovieExtendedDF, 
                        bronzeRatingsDF.bronzeRatingsDF).get_all_dataframes()

    # Data Quality Check
    # print("\n------------------------------ BRONZE LAYER DATA QUALITY CHECK ------------------------------")
    # DataQuality(bronzeMovieDF.bronzeMovieDF, 'bronzeMovieDF').data_quality()
    # DataQuality(bronzeMovieExtendedDF.bronzeMovieExtendedDF, 'bronzeMovieExtendedDF').data_quality()
    # DataQuality(bronzeRatingsDF.bronzeRatingsDF, 'bronzeRatingsDF').data_quality()
    print("\n------------------------------ SILVER LAYER DATA QUALITY CHECK ------------------------------")
    # DataQuality(OLAPDataFrames['factMovies'], 'factMovies').data_quality()
    # DataQuality(OLAPDataFrames['dimMovies'], 'dimMovies').data_quality()
    # DataQuality(OLAPDataFrames['dimMoviesGenres'], 'dimMoviesGenres').data_quality()
    #DataQuality(OLAPDataFrames['dimMoviesLanguages'], 'dimMoviesLanguages').data_quality()
    #DataQuality(OLAPDataFrames['dimMoviesCountries'], 'dimMoviesCountries').data_quality()
    # DataQuality(OLAPDataFrames['dimMoviesCompanies'], 'dimMoviesCompanies').data_quality()

    # Load to MySQL
    Load(OLAPDataFrames['factMovies'], 'fact_movies').load_to_mysql()
    Load(OLAPDataFrames['factMovies'], 'fact_movies').load_as_csv()

    Load(OLAPDataFrames['dimMovies'], 'dim_movies').load_as_csv()
    Load(OLAPDataFrames['dimMovies'], 'dim_movies').load_to_mysql()

    Load(OLAPDataFrames['dimMoviesLanguages'], 'dim_movies_languages').load_as_csv()
    Load(OLAPDataFrames['dimMoviesLanguages'], 'dim_movies_languages').load_to_mysql()

    Load(OLAPDataFrames['dimMoviesCountries'], 'dim_movies_countries').load_as_csv()
    Load(OLAPDataFrames['dimMoviesCountries'], 'dim_movies_countries').load_to_mysql()

    Load(OLAPDataFrames['dimMoviesCompanies'], 'dim_movies_companies').load_as_csv()
    Load(OLAPDataFrames['dimMoviesCompanies'], 'dim_movies_companies').load_to_mysql()

    Load(OLAPDataFrames['dimMoviesGenres'], 'dim_movies_genres').load_as_csv()
    Load(OLAPDataFrames['dimMoviesGenres'], 'dim_movies_genres').load_to_mysql()



    spark.stop()


if __name__ == "__main__":
        run_pipeline()