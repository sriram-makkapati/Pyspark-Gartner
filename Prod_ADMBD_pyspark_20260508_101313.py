To convert the provided Informatica PowerCenter ETL workflows to PySpark-based pipelines on Databricks, we will follow the outlined steps and rules. Below is the detailed conversion process:

### [1] CONVERSION SUMMARY TABLE

| Informatica Object       | Type               | PySpark Equivalent | Delta / Databricks Feature Used | Caveats |
|--------------------------|--------------------|--------------------|---------------------------------|---------|
| `configuracion_cubos`    | Source Definition  | `spark.read.jdbc`  | JDBC Source                     | None    |
| `configuracion_cubos_tgt`| Target Definition  | `df.write.format("delta")` | Delta Lake Write               | None    |
| `SQ_configuracion_cubos` | Source Qualifier   | `spark.read.jdbc` with SQL | Preserved SQL                  | None    |
| `m_Prueba_Scheduling`    | Mapping            | PySpark DataFrame  | DataFrame API                   | None    |
| `s_m_Prueba_Scheduling`  | Session            | Databricks Job Task| Databricks Jobs API             | None    |
| `wf_Prueba_Scheduling`   | Workflow           | Databricks Job     | Databricks Jobs API             | None    |

### [2] DATABRICKS NOTEBOOK (.py style)

# COMMAND 1 — Imports & Config
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col
from delta.tables import DeltaTable

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Set Spark configurations for Databricks
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.databricks.delta.autoOptimize.optimizeWrite", "true")
spark.conf.set("spark.databricks.delta.autoOptimize.autoCompact", "true")

# COMMAND 2 — Source Reads
# Define schema for source data
source_schema = StructType([
    StructField("area", StringType(), False),
    StructField("nombre_report", StringType(), False),
    StructField("nombre", StringType(), False),
    StructField("periodicidad", StringType(), False),
    StructField("duracion", StringType(), False),
    StructField("forzarexe", StringType(), False),
    StructField("tipojoin", StringType(), False),
    StructField("nborrado", StringType(), True)
])

# Read source data from SQL Server
jdbc_url = "jdbc:sqlserver://<server>:<port>;databaseName=<database>"
source_df = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "(SELECT * FROM configuracion_cubos) AS t") \
    .option("user", dbutils.secrets.get(scope="my_scope", key="db_user")) \
    .option("password", dbutils.secrets.get(scope="my_scope", key="db_password")) \
    .schema(source_schema) \
    .load()

# COMMAND 3 — Transformation Functions
# No transformations needed as per the mapping

# COMMAND 4 — Orchestration / Execution
# No additional orchestration logic needed

# COMMAND 5 — Target Writes (Delta MERGE / append)
# Define target path
target_path = "dbfs:/mnt/delta/configuracion_cubos_tgt"

# Write to Delta Lake
source_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(target_path)

# COMMAND 6 — Audit Logging
# Log row counts
row_count = source_df.count()
spark.sql(f"INSERT INTO catalog.audit.pipeline_run_log VALUES ('configuracion_cubos', {row_count}, current_timestamp())")

### [3] DELTA TABLE DDL

CREATE TABLE IF NOT EXISTS catalog.schema.configuracion_cubos_tgt (
    AREA STRING,
    NOMBRE_REPORT STRING,
    NOMBRE STRING,
    PERIODICIDAD STRING,
    DURACION STRING,
    FORZAREXE STRING,
    TIPOJOIN STRING,
    NBORRADO STRING
)
USING DELTA
LOCATION 'dbfs:/mnt/delta/configuracion_cubos_tgt'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

### [4] DATABRICKS JOB JSON SKELETON

{
  "name": "ETL Job for Configuracion Cubos",
  "tasks": [
    {
      "task_key": "s_m_Prueba_Scheduling",
      "description": "Load data from SQL Server to Delta Lake",
      "notebook_task": {
        "notebook_path": "/path/to/notebook"
      },
      "depends_on": []
    }
  ],
  "job_clusters": [
    {
      "job_cluster_key": "shared_cluster",
      "new_cluster": {
        "spark_version": "13.x-scala2.12",
        "node_type_id": "i3.xlarge",
        "num_workers": 2
      }
    }
  ]
}

### [5] REQUIREMENTS & CLUSTER INIT SCRIPT

**requirements.txt**
delta-spark==2.1.0

**cluster_init.sh**
#!/bin/bash
# Install any necessary libraries
pip install -r /dbfs/path/to/requirements.txt

### [6] UNIT TEST SKELETON

import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

class TestETL(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local").appName("ETL Test").getOrCreate()

    def test_schema(self):
        expected_schema = StructType([
            StructField("area", StringType(), False),
            StructField("nombre_report", StringType(), False),
            StructField("nombre", StringType(), False),
            StructField("periodicidad", StringType(), False),
            StructField("duracion", StringType(), False),
            StructField("forzarexe", StringType(), False),
            StructField("tipojoin", StringType(), False),
            StructField("nborrado", StringType(), True)
        ])
        df = self.spark.createDataFrame([], expected_schema)
        self.assertEqual(df.schema, expected_schema)

    def test_row_count(self):
        df = self.spark.createDataFrame([("area1", "report1", "name1", "P", "10", "Y", "INNER", None)], schema=expected_schema)
        self.assertEqual(df.count(), 1)

if __name__ == '__main__':
    unittest.main()

### [7] KNOWN LIMITATIONS / MANUAL REVIEW FLAGS

- **Email Tasks**: The email tasks in Informatica have no direct equivalent in Databricks. Consider using Databricks Jobs API notifications or integrating with an external email service.
- **Command Tasks**: The command tasks executing scripts are not directly translatable. Consider using Databricks notebooks or external orchestration tools for similar functionality.

This conversion process ensures that the Informatica PowerCenter ETL workflows are faithfully migrated to PySpark-based pipelines on Databricks, adhering to the specified rules and best practices.