from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Prueba Scheduling") \
    .getOrCreate()

# Define schema for the source data
schema = [
    "area",
    "nombre_report",
    "nombre",
    "periodicidad",
    "duracion",
    "forzarexe",
    "tipojoin",
    "nborrado"
]

# Load source data into DataFrame
source_data = [
    # Example data rows
    ("Finance", "Report1", "Monthly Report", "Monthly", "30", "Yes", "Inner", "No"),
    ("HR", "Report2", "Weekly Report", "Weekly", "7", "No", "Left", "Yes")
]

df_source = spark.createDataFrame(source_data, schema)

# Transformation logic
def transform_data(df):
    # Example transformation: filter rows where 'forzarexe' is 'Yes'
    df_filtered = df.filter(col("forzarexe") == "Yes")
    return df_filtered

# Apply transformation
df_transformed = transform_data(df_source)

# Define target schema
target_schema = [
    "AREA",
    "nombre_report",
    "nombre",
    "periodicidad",
    "duracion",
    "forzarexe",
    "tipojoin",
    "nborrado"
]

# Map transformed data to target schema
df_target = df_transformed.select(
    col("area").alias("AREA"),
    "nombre_report",
    "nombre",
    "periodicidad",
    "duracion",
    "forzarexe",
    "tipojoin",
    "nborrado"
)

# Show the transformed data
df_target.show()

# Stop Spark session
spark.stop()


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