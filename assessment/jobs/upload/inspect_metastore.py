# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC This notebook helps query the HMS to collect an inventory of what is there to help scope out the UC migration effort. It uses an external catalog API to fetch these meta data so that it doesn't do a auth against each of the storage accounts. 

# COMMAND ----------

dbutils.widgets.text("dbfs_file_path", "/tmp/uc_assessment/hms/default.csv", label="File Path")
dbutils.widgets.text("debug_mode", "True", label="Debug Mode")

# COMMAND ----------

# MAGIC %scala
# MAGIC val dbfsFilePath = dbutils.widgets.get("dbfs_file_path")
# MAGIC val filePath = "/dbfs"+ dbfsFilePath
# MAGIC val debugMode = dbutils.widgets.get("debug_mode") == "True"
# MAGIC
# MAGIC import java.io.File
# MAGIC
# MAGIC val file = new File(filePath)
# MAGIC val parentFolder = file.getParentFile
# MAGIC parentFolder.mkdirs()

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
# MAGIC import org.apache.spark.sql.catalyst.TableIdentifier
# MAGIC import java.io.{FileWriter, BufferedWriter, File}
# MAGIC
# MAGIC val bw = new BufferedWriter(new FileWriter(file, false));
# MAGIC
# MAGIC bw.write("db,table,format,type,table_location,created_version,created_time,last_access,lib,inputformat,outputformat\n")
# MAGIC val dbs = spark.sharedState.externalCatalog.listDatabases()
# MAGIC def dump() {
# MAGIC   var count = 0
# MAGIC   for( db <- dbs) {
# MAGIC     println(s"database: ${db}")
# MAGIC     val tables = spark.sharedState.externalCatalog.listTables(db)
# MAGIC
# MAGIC     for (t <- tables) {    
# MAGIC       try {
# MAGIC         //println(s"table: ${t}")
# MAGIC         val table: CatalogTable = spark.sharedState.externalCatalog.getTable(db = db, table = t)
# MAGIC         val row = s"${db},${t},${table.provider.getOrElse("Unknown")},${table.tableType.name},${table.storage.locationUri.getOrElse("None")},${table.createVersion},${table.createTime},${table.lastAccessTime},${table.storage.serde.getOrElse("Unknown")},${table.storage.inputFormat.getOrElse("Unknown")},${table.storage.outputFormat.getOrElse("Unknown")}\n"
# MAGIC         bw.write(row)
# MAGIC         count = count + 1
# MAGIC         if (debugMode == true && count > 100) {
# MAGIC           return
# MAGIC         }
# MAGIC       } catch {
# MAGIC         case e: Exception => bw.write(s"${db},${t},Unknown,Unknown,NONE,,,,,,,\n")
# MAGIC       }
# MAGIC     }
# MAGIC
# MAGIC   }
# MAGIC }
# MAGIC dump()
# MAGIC
# MAGIC bw.close

# COMMAND ----------

# MAGIC %scala
# MAGIC val df = spark.read.option("header","true").option("inferSchema","true").csv(dbfsFilePath)
# MAGIC display(df)
# MAGIC df.createOrReplaceTempView("tmptable")

# COMMAND ----------

# MAGIC %sql
# MAGIC select type, format, count(*) as tableCount
# MAGIC from tmptable where group by type, format
# MAGIC order by type, format

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ```
# MAGIC %scala
# MAGIC import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
# MAGIC import org.apache.spark.sql.catalyst.TableIdentifier
# MAGIC val table: CatalogTable = spark.sharedState.externalCatalog.getTable("accounting", "accounts_redacted")
# MAGIC table.storage.serde.getOrElse("Unknown")
# MAGIC val db = table.database
# MAGIC val t = table.identifier
# MAGIC val viewtxt = table.viewText.getOrElse("None")
# MAGIC table.storage.locationUri.getOrElse("None")
# MAGIC //val row = s"${db},${t},${table.provider.getOrElse("Unknown")},${table.tableType.name},${table.location.toString},${table.createVersion},${table.createTime},${table.lastAccessTime},${table.storage.serde.getOrElse("Unknown")},${table.storage.inputFormat.getOrElse("Unknown")},${table.storage.outputFormat.getOrElse("Unknown")}\n"
# MAGIC //print(row)
# MAGIC ```
