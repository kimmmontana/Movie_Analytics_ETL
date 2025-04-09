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

    def dim_movies_companies(self):
        """
        Business Logic

        Tables needed for the dim table:
        (1) bronzeMovieDF : movies
        (2) bronzeMovieExtendedDF : extended

        The following columns are needed in the dim table:
        (1) movie_id : bronzeMovieDF.id
        (2) companies : bronzeMovieExtendedDF.production_companies
        """
        # Join the bronzeMovieDF with bronzeMovieExtendedDF on the 'id' column
        self.dimMoviesCompaniesDF = self.bronzeMovieDF.alias('movie') \
            .join(self.bronzeMovieExtendedDf.alias('extended'), col('movie.id') == col('extended.id'), "left")
        
        # Split the 'production_companies' column by commas to convert it into an array, then explode it
        self.dimMoviesCompaniesDF = self.dimMoviesCompaniesDF.withColumn(
            "companies_array", split(col("production_companies"), ",")  # Split companies by comma
        ).withColumn(
            "company", explode(col("companies_array"))  # Explode the array into individual rows
        ).select(
            col("movie.id").alias("id"),
            col("company")  # Select the exploded company
        )

        self.dimMoviesCompaniesDF.show(10)
        pass

    def dim_movies_languages(self):
        """
        Tables needed for the dim table:
        (1) bronzeMovieExtendedDF

        Business Logic:
        (1) spoken_languages should be exploded
        (2) the output should be in 3 columns, id, language, language_iso
        """
        # Step 1: Ensure column is StringType
        df = self.bronzeMovieExtendedDf.withColumn(
            "spoken_languages", col("spoken_languages").cast(StringType())
        )

        # Step 2: Define schema of the JSON array
        language_schema = ArrayType(
            StructType([
                StructField("iso_639_1", StringType(), True),
                StructField("name", StringType(), True)
            ])
        )

        # Step 3: Parse JSON string into array of structs
        df = df.withColumn("spoken_languages_struct", from_json(col("spoken_languages"), language_schema))

        # Step 4: Explode the array of structs
        df = df.withColumn("language", explode(col("spoken_languages_struct")))

        # Step 5: Extract fields from struct
        df = df.withColumn("language_iso", col("language.iso_639_1")) \
            .withColumn("language", col("language.name"))

        # Step 6: Select final columns
        self.dimMoviesLanguagesDF = df.select("id", "language", "language_iso")

        self.dimMoviesLanguagesDF.show(10)
        pass

    def dim_movies_countries(self):
        """
        Tables needed for the dim table:
        (1) bronzeMovieExtendedDF

        Business Logic:
        (1) production_countries should be exploded
        (2) the output should be in 3 columns, id, country, country_iso
        """
        # Step 1: Ensure column is StringType
        df = self.bronzeMovieExtendedDf.withColumn(
            "production_countries", col("production_countries").cast(StringType())
        )

        # Step 2: Define schema of the JSON array
        country_schema = ArrayType(
            StructType([
                StructField("iso_3166_1", StringType(), True),
                StructField("name", StringType(), True)
            ])
        )

        # Step 3: Parse JSON string into array of structs
        df = df.withColumn("production_countries_struct", from_json(col("production_countries"), country_schema))

        # Step 4: Explode the array of structs
        df = df.withColumn("country", explode(col("production_countries_struct")))

        # Step 5: Extract fields from struct
        df = df.withColumn("country_iso", col("country.iso_3166_1")) \
            .withColumn("country", col("country.name"))

        # Step 6: Select final columns
        self.dimMoviesCountriesDF = df.select("id", "country", "country_iso")

        self.dimMoviesCountriesDF.show(10)
        pass





