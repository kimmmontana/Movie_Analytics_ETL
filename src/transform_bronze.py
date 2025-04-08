from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, IntegerType, DoubleType, ArrayType, StructType, StructField, LongType
from pyspark.sql.functions import when, col, to_date, from_json, expr, from_unixtime
from data_quality import DataQuality

class BronzeLayer:
    def __init__(self, df: DataFrame, name: str):
        self.df = df
        self.name = name

class MovieBronzeLayer(BronzeLayer):
    def __init__(self, df: DataFrame, name: str):
        super().__init__(df, name)
    
    def transform_df(self):
        """
        This method applies the transformations to the DataFrame.
        """
        # Call the data quality check before transformation
        DataQuality(self.df, self.name).data_quality()

        self.row_based_transformation()
        self.transform_id()
        self.transform_title()
        self.transform_release_date()
        self.transform_budget()
        self.transform_revenue()

        # Call the data quality check after transformation
        print("After Process:")
        DataQuality(self.df, self.name).data_quality()
        pass 

    def test_transform_df(self):
        """
        This method is used to test the transformations applied to the DataFrame.
        """
        print("*********************************before:**********************************")
        DataQuality(self.df, self.name).data_quality()
        self.transform_df()
        print("*********************************after:**********************************")
        DataQuality(self.df, self.name).data_quality()
        return None

    def test_transform_title(self):
        pass

    def test_transform_release_date(self):
        ID_CHECKER = ['862', '12110', '11860']  # Add more ids as needed
        print("*****************************************before****************************************")
        self.df.filter(col('id').isin(ID_CHECKER)).select(['id','release_date']).distinct().orderBy('release_date').show()
        
        # Count the number of rows before transformation (you can just count rows directly)
        before = self.df.count()
        
        # Apply the transformation
        self.transform_release_date()
        
        print("*****************************************after****************************************")
        self.df.filter(col('id').isin(ID_CHECKER)).select(['id','release_date']).distinct().orderBy('release_date').show()
        
        # Count the number of rows after transformation
        after = self.df.count()
        
        print(f"before: {before} after: {after}")

    def test_row_based_transformation(self):
        print("*********************************before:**********************************")
        DataQuality(self.df, self.name).data_quality()
        self.row_based_transformation()
        print("*********************************after:**********************************")
        DataQuality(self.df, self.name).data_quality()

    def test_transform_id(self):

        print("*********************************before:**********************************")
        DataQuality(self.df, self.name).data_quality()
        self.transform_movie_id()
        print("*********************************after:**********************************")
        DataQuality(self.df, self.name).data_quality()

    def test_transform_budget(self):
        print("*********************************before:**********************************")
        #print count 0s in budget column
        zeroCountBefore = DataQuality(self.df, self.name).count_zeros("budget")  
        print(f"Zero count in budget column: {zeroCountBefore}")

        #print count nulls in budget column
        nullCountBefore = DataQuality(self.df, self.name).count_nulls_1col('budget')  
        print(f"Null count in budget column: {nullCountBefore}")

        #call transform_budget method
        self.transform_budget()

        print("*********************************after:**********************************")

        zeroCountAfter = DataQuality(self.df, self.name).count_zeros("budget")  
        print(f"Zero count in budget column: {zeroCountAfter}")

        #print count nulls in budget column
        nullCountAfter = DataQuality(self.df, self.name).count_nulls_1col('budget')
        print(f"Null count in budget column: {nullCountAfter}")

    def test_transform_revenue(self):
        print("*********************************before:**********************************")
        #print count 0s in revenue column
        zeroCountBefore = DataQuality(self.df, self.name).count_zeros("revenue")  
        print(f"Zero count in revenue column: {zeroCountBefore}")

        #print count nulls in revenue column
        nullCountBefore = DataQuality(self.df, self.name).count_nulls_1col('revenue')  
        print(f"Null count in revenue column: {nullCountBefore}")

        #call transform_revenue method
        self.transform_revenue()

        print("*********************************after:**********************************")

        zeroCountAfter = DataQuality(self.df, self.name).count_zeros("revenue")  
        print(f"Zero count in revenue column: {zeroCountAfter}")

        #print count nulls in revenue column
        nullCountAfter = DataQuality(self.df, self.name).count_nulls_1col('revenue')
        print(f"Null count in revenue column: {nullCountAfter}")

    def row_based_transformation(self):
        """
        Business Logic:
        (1) remove duplicates and nulls based on movie_id column

        """
        #Remove duplicates and nulls from movie_id column
        self.df = self.df.dropDuplicates(["id"])
        self.df = self.df.filter(self.df["id"].isNotNull())

    def transform_id(self):
        """,
        Business Logic:
        (1) 1-6 numeric character of movie_id are only accepted.
        (2) Remove duplicates and nulls from movie_id column
        (3) Convert movie_id to String type.
        """
        #Filter movie_id to only accept 1-6 numeric characters
        self.df = self.df.filter(self.df["id"].rlike("^[0-9]{1,6}$"))
        
        #Remove duplicates and nulls from movie_id column
        self.df = self.df.dropDuplicates(["id"])
        self.df = self.df.filter(self.df["id"].isNotNull())
        
        #Convert movie_id to String type
        self.df = self.df.withColumn("id", self.df["id"].cast(StringType()))
        
    def transform_title(self):
        """
        Business Logic:
        (1) Remove Nulls from title column
        (2) Convert title to String type.
        (3) Duplicates are already removed by row_based_transformations
        """
        #Remove Nulls from title column
        self.df = self.df.filter(self.df["title"].isNotNull())
        
        #Convert title to String type
        self.df = self.df.withColumn("title", self.df["title"].cast(StringType()))

    def transform_release_date(self):

        """
        Business Logic:
        (1) if YYYY-MM-DD, retain, it is the chosen date format
        (2) if format is DD-MM-YYYY, transform it to YYYY-MM-DD
        (3) if format is MM/DD/YYYY, transform it to YYYY-MM-DD 
        """
        self.df = self.df.withColumn("release_date", self.df["release_date"].cast(StringType()))

        self.df = self.df.withColumn(
            "release_date",
            when(col("release_date").rlike("^\d{4}-\d{2}-\d{2}$"), to_date(col("release_date"), "yyyy-MM-dd"))
            .when(col("release_date").rlike("^\d{2}-\d{2}-\d{4}$"), to_date(col("release_date"), "dd-MM-yyyy"))
            .when(col("release_date").rlike("^\d{1,2}/\d{1,2}/\d{4}$"), to_date(col("release_date"), "MM/dd/yyyy"))
            .otherwise(None))

    def transform_budget(self):
        """
        Business Logic:
        (1) cast to double type
        (2) Convert 0 to Null
        (3) Duplicates are already removed by row_based_transformations
        """

        #cast to double type
        self.df = self.df.withColumn("budget", self.df["budget"].cast(DoubleType()))

        #convert 0 to Null
        self.df = self.df.withColumn("budget", when(col("budget") == "0", None).otherwise(col("budget")))
 
    def transform_revenue(self):
        """
        Business Logic:
        (1) cast to double type        
        (2) Convert 0 to Null 
        (3) Duplicates are already removed by row_based_transformations
        """
        #cast to double type
        self.df = self.df.withColumn("revenue", self.df["revenue"].cast(DoubleType()))

        #convert 0 to Null
        self.df = self.df.withColumn("revenue", when(col("revenue") == "0", None).otherwise(col("revenue")))
        
class MovieExtendedBronzeLayer(BronzeLayer):
    def __init__(self, df: DataFrame, name: str):
        super().__init__(df, name)
    
    def transform_df(self):
        """
        This method applies the transformations to the DataFrame.
        """
        DataQuality(self.df, self.name).data_quality()

        #Call the transformation methods in the order they should be applied
        self.row_based_transformation()
        self.transform_id()
        self.transform_genres()
        self.transform_production_companies()
        self.transform_production_countries()
        self.transform_spoken_languages()


        print("After Process:")
        DataQuality(self.df, self.name).data_quality()

    def row_based_transformation(self):
        """
        Business Logic:
        (1) remove duplicates and nulls based on movie_id column

        """
        #Remove duplicates and nulls from movie_id column
        self.df = self.df.dropDuplicates(["id"])
        self.df = self.df.filter(self.df["id"].isNotNull())
        pass

    def transform_id(self):
        """
        Business Logic:
        (1) 1-6 numeric character of movie_id are only accepted.
        (2) Remove duplicates and nulls from movie_id column
        (3) Convert movie_id to String type.
        """
        #Filter movie_id to only accept 1-6 numeric characters
        self.df = self.df.filter(self.df["id"].rlike("^[0-9]{1,6}$"))
        pass

    def transform_genres(self):
        """
        Business Logic:
        (1) Convert genres to String type.
        (2) Duplicates are already removed by row_based_transformations
        """
        #Convert genres to String type
        self.df = self.df.withColumn("genres", self.df["genres"].cast(StringType()))
        pass

    def transform_production_companies(self):
        """
        Business Logic:
        (1) Convert production_companies to String type.
        """

        #Convert production_companies to String type
        self.df = self.df.withColumn("production_companies", self.df["production_companies"].cast(StringType()))
        pass

    def transform_production_countries(self):
        """
        Business Logic:
        (1) Convert production_countries column to String type.
        (2) Extract 'iso_3166_1' key from the production_countries column into 'production_countries_iso' column.
        (3) Extract 'name' key from the production_countries column into 'production_countries' column.
        """
        # Step 1: Ensure column is StringType
        self.df = self.df.withColumn("production_countries", col("production_countries").cast(StringType()))

        # Step 2: Define schema of the JSON array
        country_schema = ArrayType(
            StructType([
                StructField("iso_3166_1", StringType(), True),
                StructField("name", StringType(), True)
            ])
        )

        # Step 3: Parse JSON string into array of structs
        self.df = self.df.withColumn("production_countries_struct", from_json(col("production_countries"), country_schema))

        # Step 4: Extract the fields into separate columns
        self.df = self.df.withColumn("production_countries_iso", col("production_countries_struct.iso_3166_1"))
        self.df = self.df.withColumn("production_countries", col("production_countries_struct.name"))

        # Step 5: Drop intermediate struct column if not needed
        self.df = self.df.drop("production_countries_struct")

        #Convert arrays to comma-separated strings
        self.df = self.df.withColumn("production_countries_iso", expr("concat_ws(',', production_countries_iso)"))
        self.df = self.df.withColumn("production_countries", expr("concat_ws(',', production_countries)"))
        pass

    def transform_spoken_languages(self):
        """
        Business Logic:
        (1) Convert spoken_languages column to String type.
        (2) Extract 'iso_639_1' key from the spoken_languages column into 'spoken_languages_iso' column.
        (3) Extract 'name' key from the spoken_languages column into 'spoken_languages' column.
        """
        # Step 1: Ensure column is StringType
        self.df = self.df.withColumn("spoken_languages", col("spoken_languages").cast(StringType()))

        # Step 2: Define schema of the JSON array
        language_schema = ArrayType(
            StructType([
                StructField("iso_639_1", StringType(), True),
                StructField("name", StringType(), True)
            ])
        )

        # Step 3: Parse JSON string into array of structs
        self.df = self.df.withColumn("spoken_languages_struct", from_json(col("spoken_languages"), language_schema))

        # Step 4: Extract the fields into separate columns
        self.df = self.df.withColumn("spoken_languages_iso", col("spoken_languages_struct.iso_639_1"))
        self.df = self.df.withColumn("spoken_languages", col("spoken_languages_struct.name"))

        # Step 5: Drop intermediate struct column if not needed
        self.df = self.df.drop("spoken_languages_struct")

        #Convert arrays to comma-separated strings
        self.df = self.df.withColumn("spoken_languages_iso", expr("concat_ws(',', spoken_languages_iso)"))
        self.df = self.df.withColumn("spoken_languages", expr("concat_ws(',', spoken_languages)"))
        pass

class RatingsBronzeLayer(BronzeLayer):
    def __init__(self, df: DataFrame, name: str):
        super().__init__(df, name)
        pass

    def transform_df(self):
        """
        This method applies the transformations to the DataFrame.
        """
        # Call the data quality check before transformation
        DataQuality(self.df, self.name).data_quality()

        # Call the transformation methods in the order they should be applied
        self.transform_id()
        self.transform_avg_rating()
        self.transform_total_ratings()
        self.transform_total_std_dev()
        self.transform_last_rated()

        # Call the data quality check after transformation
        print("After Process:") 
        DataQuality(self.df, self.name).data_quality()

    def transform_id(self):
        """
        Business Logic:
        (1) 1-6 numeric character of movie_id are only accepted.
        (3) Convert id to String type.
        """
        # Filter movie_id to only accept 1-6 numeric characters
        self.df = self.df.filter(self.df["id"].rlike("^[0-9]{1,6}$"))

        #convert id to String type
        self.df = self.df.withColumn("id", self.df["id"].cast(StringType()))
        pass

    def transform_avg_rating(self):
        """
        Business Logic:
        (1) cast to double type
        """
        # cast to double type
        self.df = self.df.withColumn("avg_rating", self.df["avg_rating"].cast(DoubleType()))
        pass

    def transform_total_ratings(self):
        """
        Business Logic:
        (1) cast to integer type
        """
        # cast to integer type
        self.df = self.df.withColumn("total_ratings", self.df["total_ratings"].cast(IntegerType()))
        pass

    def transform_total_std_dev(self):
        """
        Business Logic:
        (1) cast to double type
        """
        # cast to double type
        self.df = self.df.withColumn("std_dev", self.df["std_dev"].cast(DoubleType()))
        pass

    def transform_last_rated(self):
        """
        Business Logic:
        (1) Convert Unix timestamp to readable datetime
        (2) Convert datetime to date (yyyy-MM-dd)
        """
        # Convert Unix timestamp (seconds) to timestamp string
        self.df = self.df.withColumn("last_rated", from_unixtime(col("last_rated")))

        # Convert to date type
        self.df = self.df.withColumn("last_rated", to_date(col("last_rated")))

        pass

