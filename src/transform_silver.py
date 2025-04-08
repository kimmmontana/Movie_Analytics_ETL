from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, IntegerType, DoubleType, ArrayType, StructType, StructField, LongType
from pyspark.sql.functions import when, col, split, explode, to_date, from_json, expr
from data_quality import DataQuality

class SilverLayer:
    def __init__(self, bronzeMovieDF: DataFrame, bronzeMovieExtendedDF: DataFrame, bronzeRatingsDF: DataFrame):
        #initialize the class with the DataFrames
        self.bronzeMovieDF = bronzeMovieDF
        self.bronzeMovieExtendedDf = bronzeMovieExtendedDF
        self.bronzeRatingsDF = bronzeRatingsDF

        #output DataFrames
        self.factMoviesDF = None
        self.dimMoviesDF = None
        self.dimMoviesGenresDF = None
        self.dimMoviesLanguagesDF = None
        self.dimMoviesCountriesDF = None
        self.dimMoviesCompaniesDF = None
        
    def fact_movies(self):
       """
       Business Logic:
        Tables needed for the fact table:
        (1) bronzeMovieDF : movies
        (2) bronzeRatingsDF : ratings

       The following column is needed in the fact table:
       (1) id : bronzeMovieDF.id
       (2) release_date : bronzeMovieDF.release_date
       (3) budget : bronzeMovieDF.budget
       (4) revenue : bronzeMovieDF.revenue
       (5) profit : bronzeMovieDF.revenue - bronzeMovieDF.budget iff not NULL
       (6) avg_rating : bronzeRatingsDF.avg_rating
       (7) total_ratings : bronzeRatingsDF.total_ratings
       (8) std_dev : bronzeRatingsDF.std_dev
       (9) last_rated : bronzeRatingsDF.last_rated
       """
       self.factMoviesDF = self.bronzeMovieDF.alias('movie') \
        .join(self.bronzeRatingsDF.alias('ratings'), col('movie.id') == col('ratings.id'), "left")
       
       self.factMoviesDF = self.factMoviesDF.select(
           col("movie.id").alias("movie_id"),
           col("release_date"),
           col("budget"),
           col("revenue"),
        when(
            (col("movie.revenue").isNotNull()) & (col("movie.budget").isNotNull()), 
            (col("movie.revenue") - col("movie.budget"))
        ).otherwise(None).alias("profit"),
           col("avg_rating"),
           col("total_ratings"),
           col("std_dev"),
           col("last_rated")
       )
       self.factMoviesDF.show(10) 
       pass

    def dim_movies(self):
        """
        Business Logic:
        
        Needed for the dim table:
        (1) bronzeMovieDF : movies
        (2) bronzeRatingsDF : ratings

        The following columns are needed in the dim table:
        (1) movie_id : bronzeMovieDF.id
        (2) title : bronzeMovieDF.title
        (3) release_date : bronzeMovieDF.release_date
        (4) last_rated : bronzeRatingsDF.last_rated
        
        """
        self.dimMoviesDF = self.bronzeMovieDF.alias('movie') \
            .join(self.bronzeRatingsDF.alias('ratings'), col('movie.id') == col('ratings.id'), "left")
        
        self.dimMoviesDF = self.dimMoviesDF.select(
            col("movie.id").alias("movie_id"),
            col("title"),
            col("release_date"),
            col("last_rated")
        )

        self.dimMoviesDF.show(10)
        pass

    def dim_movies_genres(self):
        """
        Business Logic:
        Tables needed for the dim table:
        (1) bronzeMovieDF : movies
        (2) bronzeMovieExtendedDF : extended

        The following columns are needed in the dim table:
        (1) movie_id : bronzeMovieDF.id
        (2) genres : bronzeMovieDF.genres
        """
        # Join the bronzeMovieDF with bronzeMovieExtendedDF on the 'id' column
        self.dimMoviesGenresDF = self.bronzeMovieDF.alias('movie') \
            .join(self.bronzeMovieExtendedDf.alias('extended'), col('movie.id') == col('extended.id'), "left")

        # Split the 'genres' column by commas to convert it into an array, then explode it
        self.dimMoviesGenresDF = self.dimMoviesGenresDF.withColumn(
            "genres_array", split(col("genres"), ",")  # Split genres by comma
        ).withColumn(
            "genre", explode(col("genres_array"))  # Explode the array into individual rows
        ).select(
            col("movie.id").alias("id"),
            col("genre")  # Select the exploded genre
        )

        self.dimMoviesGenresDF.show(10)
        pass

    def dim_movies_languages(self):
        """
        Business Logic:
        The following columns are needed in the dim table:
        (1) movie_id : bronzeMovieDF.id
        (2) languages : bronzeMovieDF.languages
        """
        pass

    def dim_movies_countries(self):
        """
        Business Logic:
        The following columns are needed in the dim table:
        (1) movie_id : bronzeMovieDF.id
        (2) countries : bronzeMovieDF.countries
        """
        pass

    def dim_movies_companies(self):
        """
        Business Logic:
        The following columns are needed in the dim table:
        (1) movie_id : bronzeMovieDF.id
        (2) companies : bronzeMovieDF.companies
        """
        pass



