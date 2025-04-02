from pyspark.sql import SparkSession

class DataExtractor:
    def __init__(self, spark: SparkSession):
        """
        Initializes the DataExtractor with a Spark session.
        """
        self.spark = spark

    def extract_data(self, file_path: str, file_format: str, **options):
        """
        Reads data from a file into a PySpark DataFrame.
        
        :param file_path: Path to the data file.
        :param file_format: File format (csv, json, parquet, etc.).
        :param options: Additional options like header, inferSchema.
        :return: PySpark DataFrame
        """
        if file_format == "csv":
            return self.spark.read.csv(file_path, **options)
        elif file_format == "json":
            return self.spark.read.json(file_path, **options)
        else:
            raise ValueError(f"Unsupported file format: {file_format}")
