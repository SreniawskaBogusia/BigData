// Databricks notebook source
// MAGIC %md-sandbox
// MAGIC ## Architektura Lambda 
// MAGIC 
// MAGIC Architektura Lambda służy do przetważanie wsadowego i w czasie rzeczywistym (batch- and real-time).
// MAGIC Zawiera niezmienne (immutable) źródło danych, które służy tylko jako system ewidencji. Zdarzenia z sygnaturą czasową są dołączane do
// MAGIC istniejące wydarzenia (nic nie jest nadpisywane). Dane są domyślnie uporządkowane według czasu przybycia.
// MAGIC 
// MAGIC Wykres przedstawia dwa pipeliny jeden batch i drugi streaming. 
// MAGIC 
// MAGIC Proces łączenia przetważanie wsadowego i strumieniowego jest bardzo trudny dlatego często taktuje się je jako osobne processy a połączenie można zrobić na koncu procesu.
// MAGIC 
// MAGIC 
// MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/Delta/lambda.png" style="height: 400px"/></div><br/>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ## Databricks Delta Architecture
// MAGIC 
// MAGIC * Źródła danych są umieszczane a tabeli ”bronze” to dotyczy również streamu.
// MAGIC * Tabela 'Raw' jest przetwarzana do tabeli ”silver”. Ta tabelę możesz łączyć z tabelami wymiarów.
// MAGIC * Tabela końcowa z wynikami analitycznymi „gold” jest użyta do analizy. Są w niej dane gotowe do analizy, oczyszczone i zawierające aggregacje (tabela Fact)
// MAGIC 
// MAGIC 
// MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/Delta/delta.png" style="height: 350px"/></div><br/>

// COMMAND ----------

val basePath       = "mnt/streaming"
val bronzePath     = "/EditsRaw.delta"
val silverPath     = "/Edits.delta"
val goldPath       = "/EditsSummary.delta"
val checkpointPath = "/checkpoints"

// COMMAND ----------

// MAGIC %md
// MAGIC ## Tabela RAW ("bronze")
// MAGIC 
// MAGIC <b>Surowe dane</b> dane w niezmienionej postaci wrzucone jako 'bulk' lub jako dane przesyłowe.

// COMMAND ----------

import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType}

// use lazy keyword to suppress gobs of output
lazy val schema = StructType(List(
  StructField("channel", StringType, true),
  StructField("comment", StringType, true),
  StructField("delta", IntegerType, true),
  StructField("flag", StringType, true),
  StructField("geocoding", StructType(List(           
    StructField("city", StringType, true),
    StructField("country", StringType, true),
    StructField("countryCode2", StringType, true),
    StructField("countryCode3", StringType, true),
    StructField("stateProvince", StringType, true),
    StructField("latitude", DoubleType, true),
    StructField("longitude", DoubleType, true)
  )), true),
  StructField("isAnonymous", BooleanType, true),
  StructField("isNewPage", BooleanType, true),
  StructField("isRobot", BooleanType, true),
  StructField("isUnpatrolled", BooleanType, true),
  StructField("namespace", StringType, true),           
  StructField("page", StringType, true),                
  StructField("pageURL", StringType, true),             
  StructField("timestamp", TimestampType, true),   
  StructField("url", StringType, true),
  StructField("user", StringType, true), 
  StructField("userURL", StringType, true),
  StructField("wikipediaURL", StringType, true),
  StructField("wikipedia", StringType, true)
))

// COMMAND ----------

// MAGIC %md
// MAGIC Zapis danych do ścieżki bronze (delta).

// COMMAND ----------

import org.apache.spark.sql.functions.from_json
spark.readStream
  .format("kafka")  
  .option("kafka.bootstrap.servers", "server1......") 
  .option("subscribe", "en")
  .load()
  .withColumn("json", from_json($"value".cast("string"), schema))
  .select($"timestamp".alias("kafka_timestamp"), $"json.*")
  .writeStream
  .format("delta")
  .option("checkpointLocation", checkpointPath + "/bronze")
  .outputMode("append")
  .start(bronzePath)

// COMMAND ----------

// MAGIC %md
// MAGIC Po zainicjowaniu tworzmymy table.

// COMMAND ----------

spark.sql("DROP TABLE IF EXISTS EditsRaw")

spark.sql(s"""
  CREATE TABLE EditsRaw
  USING Delta
  LOCATION '$bronzePath'
""")

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM EditsRaw LIMIT 5

// COMMAND ----------

// MAGIC %md
// MAGIC ## Tabela QUERY ("silver")
// MAGIC 
// MAGIC Stream idzie do Databricks Delta.

// COMMAND ----------

import org.apache.spark.sql.functions.unix_timestamp

spark.readStream
  .format("delta")
  .load(bronzePath)
  .select($"wikipedia",
          $"isAnonymous",
          $"namespace",
          $"page",
          $"pageURL",
          $"geocoding",
          unix_timestamp($"timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSSX").cast("timestamp").as("timestamp"),
          $"user")
  .writeStream
  .format("delta")
  .option("checkpointLocation", checkpointPath + "/silver")
  .outputMode("append")
  .start(silverPath)

// COMMAND ----------

// MAGIC %md
// MAGIC Poczekaj na inicjalizację streamu

// COMMAND ----------

spark.sql("DROP TABLE IF EXISTS WikipediaEdits")
spark.sql(s"""
  CREATE TABLE WikipediaEdits
  USING Delta
  LOCATION '$silverPath'
""") 

// COMMAND ----------

// MAGIC %md
// MAGIC Można podejrzeć widok streamu.

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM WikipediaEdits

// COMMAND ----------

// MAGIC %md
// MAGIC ## Tabela aggregacji ("gold") 
// MAGIC 
// MAGIC Aggregacje mogą być wolne.

// COMMAND ----------

import org.apache.spark.sql.functions.desc

val goldDF = spark.readStream
  .format("delta")
  .load(silverPath)
  .withColumn("countryCode", $"geocoding.countryCode3")
  .filter($"namespace"==="article")
  .filter($"countryCode" =!= "null")
  .filter($"isAnonymous" === true)
  .groupBy($"countryCode")
  .count()
  .withColumnRenamed("count", "total")
  .orderBy($"total".desc)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC 
// MAGIC ## Wizualizacje ("platinum") 
// MAGIC 
// MAGIC 
// MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/Delta/ch5-plot-options.png" style="height: 200px"/></div><br/>
// MAGIC 
// MAGIC  
// MAGIC Akcja `display` tworzy wizualizacje na żywo!

// COMMAND ----------

display(goldDF)

// COMMAND ----------

// MAGIC %md
// MAGIC Zatrzymaj stream.

// COMMAND ----------

import org.apache.spark.sql.streaming.StreamingQuery
spark.streams.active.foreach((s: StreamingQuery) => s.stop())

dbutils.fs.rm(userhome, true)