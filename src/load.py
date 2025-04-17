from pyspark.sql import DataFrame

class Load:
    def __init__(self, df: DataFrame, table_name: str):
        self.df = df
        self.table_name = table_name

    def load_to_mysql(self):
        """
        Load the DataFrame to MySQL database.
        """
        jdbc_url = "jdbc:mysql://localhost:3306/movie_analytics_etl"
        properties = {
            "user": "root",
            "password": "admin_kimmontana",
            "driver": "com.mysql.cj.jdbc.Driver"
        }

        self.df.write.jdbc(url=jdbc_url, table=self.table_name, mode="overwrite", properties=properties)
        print(f"Data loaded to MySQL table: {self.table_name}")


    def load_as_csv(self):
        """
        Load the DataFrame as CSV files.
        """
        output_path = f"../outputs/{self.table_name}"
        self.df.write.csv(output_path, header=True, mode="overwrite")
        print(f"Data loaded to CSV files at: {output_path}")
