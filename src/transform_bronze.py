from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, IntegerType, DoubleType
from pyspark.sql.functions import when, col, to_date
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
        self.transform_movie_id()
        self.transform_title()
        self.transform_release_date()
        self.transform_budget()
        self.transform_revenue()

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

    def transform_movie_id(self):
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


        self.row_based_transformation()
        self.transform_id()



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
        
        pass

    def transform_production_companies(self):
        pass

    def transform_production_countries(self):
        pass

    def transform_spoken_language(self):
        pass

class RatingsBronzeLayer(BronzeLayer):
    def __init__(self, df: DataFrame, name: str):
        super().__init__(df, name)
        pass

    def transform_movie_id(self):
        pass

    def transform_avg_rating(self):
        pass

    def transform_total_ratings(self):
        pass

    def transform_total_std_dev(self):
        pass

    def transform_last_rated(self):
        pass

