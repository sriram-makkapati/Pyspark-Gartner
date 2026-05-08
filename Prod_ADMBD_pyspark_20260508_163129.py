from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Prueba Scheduling") \
        .getOrCreate()

    # Load source data
    # Assuming data is loaded from a CSV file or database table
    source_data = spark.read.csv("path_to_source_data.csv", header=True, inferSchema=True)

    # Transformation logic
    transformed_data = source_data.select(
        col("area").alias("AREA"),
        col("nombre_report"),
        col("nombre"),
        col("periodicidad"),
        col("duracion"),
        col("forzarexe"),
        col("tipojoin"),
        col("nborrado")
    )

    # Load transformed data to target
    # Assuming target is a database table or another file
    transformed_data.write.mode("overwrite").csv("path_to_target_data.csv")

if __name__ == "__main__":
    main()

This code initializes a Spark session, reads the source data, applies the necessary transformations, and writes the transformed data to the target location. Adjust the paths and data sources as needed for your specific environment.


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def create_spark_session():
    return SparkSession.builder \
        .appName("Info Objetos DB Migration") \
        .getOrCreate()

def extract_data(spark):
    # Define schema for the source data
    schema = StructType([
        StructField("tabname", StringType(), True),
        StructField("tabid", IntegerType(), True),
        StructField("dbspace", StringType(), True),
        StructField("partnum", IntegerType(), True),
        StructField("dbname", StringType(), True),
        StructField("tipo", StringType(), True),
        StructField("spcused", IntegerType(), True),
        StructField("extents", IntegerType(), True),
        StructField("conblob", StringType(), True)
    ])
    
    # Simulate data extraction from source
    data = [
        ("table1", 1, "space1", 100, "db1", "type1", 200, 10, "blob1"),
        ("table2", 2, "space2", 101, "db2", "type2", 300, 20, "blob2"),
        # Add more rows as needed
    ]
    
    return spark.createDataFrame(data, schema)

def transform_data(df):
    # Implement any transformation logic if needed
    # Currently, the transformation is a direct mapping
    return df

def load_data(df):
    # Simulate loading data to target
    df.show()

def main():
    spark = create_spark_session()
    df = extract_data(spark)
    transformed_df = transform_data(df)
    load_data(transformed_df)

if __name__ == "__main__":
    main()