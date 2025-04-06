from pyspark.sql import DataFrame
from pyspark.sql.functions import col
import pandas as pd

class DataQuality:
    def __init__(self, df: DataFrame, name: str):
        self.df = df
        self.name = name

    def data_quality(self):
        print(f"\n-------------- DATA PROFILE REPORT FOR {self.name} ---------------")
        
        # Print schema of the DataFrame
        print("\nSchema:")
        self.df.printSchema()
        self.df.describe().show()

        # Collect data quality metrics
        null_counts = self.count_nulls()
        duplicate_count = self.count_duplicates()
        row_count = self.count_rows()
        column_count = self.count_columns()

        print(f"Total Rows: {row_count}")
        print(f"Total Columns: {column_count}")

        print(f"\n-------------- UNIQUENESS/DUPLICATE CHECK FOR {self.name} ---------------")
        print(f"\nDuplicate Rows: {duplicate_count}")


        # Displaying data quality summary as a table
        print(f"\n----------------- COMPLETENESS/NULL CHECK for {self.name} --------------------------")
        self.print_table("Null Counts", null_counts)

        
        # Display sample data
        print(f"-----------------------------SAMPLE DATA OF {self.name}-----------------------------")
        self.df.show(5)
        return None

    def print_table(self, title, data):
        """Helper function to print data as a table"""
        df = pd.DataFrame(list(data.items()), columns=["Column", title])
        print(df.to_string(index=False))
    
    def count_nulls(self):
        null_counts = {col_name: self.df.filter(col(col_name).isNull()).count() for col_name in self.df.columns}
        #print(f"Null Counts in {self.name}: {null_counts}")
        return null_counts
    
    def count_duplicates(self):
        duplicate_count = self.df.count() - self.df.dropDuplicates(['id']).count()
        #print(f"Duplicate Rows in {self.name}: {duplicate_count}")
        return duplicate_count
    
    def count_rows(self):
        row_count = self.df.count()
        #print(f"Total Rows in {self.name}: {row_count}")
        return row_count
    
    def count_columns(self):
        column_count = len(self.df.columns)
        #print(f"Total Columns in {self.name}: {column_count}")
        return column_count
    
    def count_zeros(self, column_name):
        zero_count = self.df.filter(col(column_name) == 0).count()
        return zero_count
    
    def count_nulls_1col(self, column_name):
        null_count = self.df.filter(col(column_name).isNull()).count()
        return null_count



    
    