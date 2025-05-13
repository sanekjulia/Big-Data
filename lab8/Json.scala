// Databricks notebook source
// MAGIC %fs ls  /FileStore/tables/json/

// COMMAND ----------

val df = spark.read.format("json").load("dbfs:/FileStore/tables/json/brzydki.json")

// COMMAND ----------

val df = spark.read.format("json")
.option("multiline","true")
.load("dbfs:/FileStore/tables/json/brzydki.json")

// COMMAND ----------

// MAGIC %md
// MAGIC Wczytanie 10 atrybutów z pliku brzydki.json

// COMMAND ----------

import org.apache.spark.sql.functions._


val selectedDf = df
  .withColumn("feature", explode($"features"))
  .selectExpr(
    "jobDetails.jobId as jobId",
    "jobDetails.changesTimestamp as changesTimestamp",
    "feature.type as featureType",
    "feature.geometry.type as geometryType",
    "feature.geometry.coordinates as coordinates",
    "feature.properties.featureId as featureId",
    "feature.properties.featureGroup as featureGroup",
    "feature.properties.baseFormComponent.form as baseForm",
    "feature.properties.baseFormComponent.versionNumber as baseFormVersion",
    "feature.properties.baseFormComponent.versionCreationMetadata.editDate as baseFormEditDate"
  )

//selectedDf.show(false)
display(selectedDf)

// COMMAND ----------

display(df)

// COMMAND ----------

df.selectExpr(
   "feature.geometry",
    "feature.properties.featureGroup as featureGroup")

// COMMAND ----------

// In Scala
val file = "/databricks-datasets/learning-spark-v2/flights/summary-data/json/*"
val df = spark.read.format("json").load(file)



// COMMAND ----------

// MAGIC %python
// MAGIC # In Python
// MAGIC file = "/databricks-datasets/learning-spark-v2/flights/summary-data/json/*"
// MAGIC df = spark.read.format("json").load(file)

// COMMAND ----------

// MAGIC %md Tworzenie Tabeli z pliku JSON
// MAGIC

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
// MAGIC USING json
// MAGIC OPTIONS (
// MAGIC path "/databricks-datasets/learning-spark-v2/flights/summary-data/json/*"
// MAGIC )

// COMMAND ----------

// In Scala/Python
spark.sql("SELECT * FROM us_delay_flights_tbl").show()

// COMMAND ----------

// In Scala
df.write.format("json")
.mode("overwrite")
.option("compression", "snappy")
.save("/tmp/data/json/df_json")



// COMMAND ----------

# In Python
(df.write.format("json")
.mode("overwrite")
.option("compression", "snappy")
.save("/tmp/data/json/df_json"))

// COMMAND ----------

spark.conf.set("spark.sql.avro.compression.codec","deflate")

// COMMAND ----------

