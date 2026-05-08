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
# Read from SQL Server for configuracion_cubos
jdbc_url = "jdbc:sqlserver://<server>:<port>;databaseName=<database>"
jdbc_properties = {
    "user": dbutils.secrets.get(scope="<scope>", key="<user_key>"),
    "password": dbutils.secrets.get(scope="<scope>", key="<password_key>"),
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

configuracion_cubos_df = spark.read.jdbc(
    url=jdbc_url,
    table="(SELECT area, nombre_report, nombre, periodicidad, duracion, forzarexe, tipojoin, nborrado FROM dbo.configuracion_cubos) AS configuracion_cubos",
    properties=jdbc_properties
)

# Read from Informix for query_objetos
informix_jdbc_url = "jdbc:informix-sqli://<server>:<port>/<database>:INFORMIXSERVER=<server_name>"
informix_jdbc_properties = {
    "user": dbutils.secrets.get(scope="<scope>", key="<user_key>"),
    "password": dbutils.secrets.get(scope="<scope>", key="<password_key>"),
    "driver": "com.informix.jdbc.IfxDriver"
}

query_objetos_df = spark.read.jdbc(
    url=informix_jdbc_url,
    query="""--Tablas sin fragmentación que no tengan campos blob/clob
             select a.tabname , b.tabid, dbinfo('dbspace',a.partnum) dbspace, a.partnum, a.dbsname, 'T' tipo,
             (d.ti_npdata*d.ti_pagesize/1024), d.ti_nextns, 'N' as conblob
             from sysmaster:systabnames a, systables b, sysmaster:systabinfo d
             where b.tabid  > 99
             and a.partnum = b.partnum
             and a.partnum=d.ti_partnum
             and b.tabtype = 'T' --Sólo tablas
             and b.partnum != 0 --Que no estén particionadas
             and not exists (select colname from syscolumns
             where coltype in (41,297) and extended_id in (10,11)
             and tabid = b.tabid)
             union
             --Tablas sin fragmentación que sí tengan campos blob/clob
             select a.tabname , b.tabid, dbinfo('dbspace',a.partnum) dbspace, a.partnum, a.dbsname, 'T' tipo,
             (d.ti_npdata*d.ti_pagesize/1024), d.ti_nextns, 'S' as conblob
             from sysmaster:systabnames a, systables b, sysmaster:systabinfo d, syscolumns e
             where b.tabid  > 99
             and a.partnum = b.partnum
             and a.partnum=d.ti_partnum
             and b.tabtype = 'T' --Sólo tablas
             and b.partnum != 0 --Que no estén particionadas
             and b.tabid = e.tabid
             and (e.coltype in (41,297) and e.extended_id in (10,11))
             union
             --Tablas fragmentadas sin blob
             select a.tabname, b.tabid, dbinfo('dbspace',a.partnum) dbspace, a.partnum , a.dbsname,'TF' tipo,
             (d.ti_npdata*d.ti_pagesize/1024), d.ti_nextns, 'N' as conblob
             from sysmaster:systabnames a, systables b, sysfragments c, sysmaster:systabinfo d
             where a.partnum = c.partn
             and a.partnum=d.ti_partnum
             and b.tabid = c.tabid
             and c.fragtype = 'T'
             and not exists (select colname from syscolumns
             where coltype in (41,297) and extended_id in (10,11)
             and tabid = b.tabid)
             union
             --Tablas fragmentadas con blob
             select a.tabname, b.tabid, dbinfo('dbspace',a.partnum) dbspace, a.partnum , a.dbsname,'TF' tipo,
             (d.ti_npdata*d.ti_pagesize/1024), d.ti_nextns, 'S' as conblob
             from sysmaster:systabnames a, systables b, sysfragments c, sysmaster:systabinfo d , syscolumns e
             where a.partnum = c.partn
             and a.partnum=d.ti_partnum
             and b.tabid = c.tabid
             and c.fragtype = 'T'
             and b.tabid = e.tabid
             and (e.coltype in (41,297) and e.extended_id in (10,11))
             union
             --Tamaño de índices detached sin fragmentar
             select a.tabname,  b.tabid, dbinfo('dbspace',a.partnum) dbspace, a.partnum, a.dbsname,'I' tipo,
             (d.ti_npused*d.ti_pagesize/1024), d.ti_nextns, 'N' as conblob
             from sysmaster:systabnames a, sysfragments b, sysmaster:systabinfo d
             where b.tabid > 99
             and b.fragtype = 'I'
             and b.strategy ='I'
             and a.partnum=d.ti_partnum
             and a.partnum = b.partn
             and a.tabname = b.indexname
             union
             --Tamaño de índices detached fragmentados
             select a.tabname,  b.tabid, dbinfo('dbspace',a.partnum) dbspace, a.partnum, a.dbsname,'IF' tipo,
             (d.ti_npused*d.ti_pagesize/1024), d.ti_nextns, 'N' as conblob
             from sysmaster:systabnames a, sysfragments b, sysmaster:systabinfo d
             where b.tabid > 99
             and b.fragtype = 'I'
             and b.strategy !='I'
             and a.partnum=d.ti_partnum
             and a.partnum = b.partn
             and a.tabname = b.indexname
             union   -- Cálculo APROXIMADO del tamaño de los indices attached
             select  a.idxname, a.tabid, dbinfo('dbspace',b.partnum) dbspace, b.partnum, d.dbsname, 'IA' tipo,
             ((c.ti_npused-c.ti_npdata)*c.ti_pagesize/1024), c.ti_nextns, 'N' as conblob
             from sysindices a, systables b, sysmaster:systabinfo c, sysmaster:systabnames d
             where a.tabid = b.tabid
             and b.partnum = d.partnum
             and b.partnum = c.ti_partnum
             and a.idxname not in (select indexname from sysfragments
             where tabid = a.tabid)
             and a.tabid > 99""",
    properties=informix_jdbc_properties
)

# COMMAND 3 — Transformation Functions
# No transformations are defined in the Informatica mapping, so this section is omitted.

# COMMAND 4 — Target Writes
# Write to Delta Lake for configuracion_cubos_tgt
configuracion_cubos_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("catalog.schema.configuracion_cubos_tgt")

# Write to Delta Lake for objetos_informix
query_objetos_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("catalog.schema.objetos_informix")