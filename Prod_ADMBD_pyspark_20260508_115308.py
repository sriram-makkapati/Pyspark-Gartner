# COMMAND 1 — Imports & Config
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os

spark = SparkSession.builder.getOrCreate()

# Set Databricks-specific configurations
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.databricks.delta.autoOptimize.optimizeWrite", "true")
spark.conf.set("spark.databricks.delta.autoOptimize.autoCompact", "true")

# COMMAND 2 — Source Reads
# Source: configuracion_cubos
jdbc_url = "jdbc:sqlserver://<server>:<port>;databaseName=<database>"
jdbc_properties = {
    "user": dbutils.secrets.get(scope="<scope>", key="<key>"),
    "password": dbutils.secrets.get(scope="<scope>", key="<key>"),
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

configuracion_cubos_df = spark.read.jdbc(
    url=jdbc_url,
    table="dbo.configuracion_cubos",
    properties=jdbc_properties
)

# Source: query_objetos with custom SQL
query_objetos_sql = """
SELECT a.tabname, b.tabid, dbinfo('dbspace', a.partnum) AS dbspace, a.partnum, a.dbsname, 'T' AS tipo,
(d.ti_npdata * d.ti_pagesize / 1024) AS spcused, d.ti_nextns, 'N' AS conblob
FROM sysmaster:systabnames a, systables b, sysmaster:systabinfo d
WHERE b.tabid > 99
AND a.partnum = b.partnum
AND a.partnum = d.ti_partnum
AND b.tabtype = 'T'
AND b.partnum != 0
AND NOT EXISTS (SELECT colname FROM syscolumns WHERE coltype IN (41, 297) AND extended_id IN (10, 11) AND tabid = b.tabid)
UNION
SELECT a.tabname, b.tabid, dbinfo('dbspace', a.partnum) AS dbspace, a.partnum, a.dbsname, 'T' AS tipo,
(d.ti_npdata * d.ti_pagesize / 1024) AS spcused, d.ti_nextns, 'S' AS conblob
FROM sysmaster:systabnames a, systables b, sysmaster:systabinfo d, syscolumns e
WHERE b.tabid > 99
AND a.partnum = b.partnum
AND a.partnum = d.ti_partnum
AND b.tabtype = 'T'
AND b.partnum != 0
AND b.tabid = e.tabid
AND (e.coltype IN (41, 297) AND e.extended_id IN (10, 11))
"""

query_objetos_df = spark.read.jdbc(
    url=jdbc_url,
    table=f"({query_objetos_sql}) AS query_objetos",
    properties=jdbc_properties
)

# COMMAND 3 — Transformation Functions
# No transformations are defined in the Informatica mappings, so this section is omitted.

# COMMAND 4 — Target Writes
# Target: configuracion_cubos_tgt
configuracion_cubos_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("catalog.schema.configuracion_cubos_tgt")

# Target: objetos_informix
query_objetos_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("catalog.schema.objetos_informix")