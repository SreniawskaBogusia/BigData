// Databricks notebook source
// MAGIC %md
// MAGIC # Zadanie 1

// COMMAND ----------

// MAGIC %python
// MAGIC actorsUrl = "https://raw.githubusercontent.com/cegladanych/azure_bi_data/main/IMDB_movies/actors.csv"
// MAGIC filePath = "/FileStore/tables/Files/"
// MAGIC dbutils.fs.mkdirs(filePath)
// MAGIC actorsFile = "actors.csv"
// MAGIC tmp = "file:/tmp/"
// MAGIC dbfsdestination = "dbfs:/FileStore/tables/Files/"

// COMMAND ----------

// MAGIC %python
// MAGIC import urllib.request
// MAGIC 
// MAGIC 
// MAGIC urllib.request.urlretrieve(actorsUrl,"/tmp/" + actorsFile)
// MAGIC dbutils.fs.mv(tmp + actorsFile,dbfsdestination + actorsFile)

// COMMAND ----------

// MAGIC %scala
// MAGIC import org.apache.spark.sql.types.{IntegerType,StringType, StructType,StructField}
// MAGIC val schemat = StructType(Array(
// MAGIC StructField("imbd_title_id", StringType, false),
// MAGIC StructField("ordering", IntegerType, false),
// MAGIC StructField("imbd_name_id", StringType, false),
// MAGIC StructField("category", StringType, false),
// MAGIC StructField("job", StringType, false),
// MAGIC StructField("characters", StringType, false)
// MAGIC )) 

// COMMAND ----------

// MAGIC 
// MAGIC %scala
// MAGIC val filePath = "dbfs:/FileStore/tables/Files/actors.csv"
// MAGIC val actorsDf = spark.read.format("csv")
// MAGIC             .option("header","true")
// MAGIC             .schema(schemat)
// MAGIC             .load(filePath)

// COMMAND ----------

// MAGIC   %md
// MAGIC   ## Zadanie 2

// COMMAND ----------

// MAGIC %scala
// MAGIC import org.apache.spark.sql.types.{IntegerType,StringType, StructType,StructField}
// MAGIC val schemat = StructType(Array(
// MAGIC StructField("imbd_title_id", StringType, false),
// MAGIC StructField("ordering", IntegerType, false),
// MAGIC StructField("imbd_name_id", StringType, false),
// MAGIC StructField("category", StringType, false),
// MAGIC StructField("job", StringType, false),
// MAGIC StructField("characters", StringType, false)
// MAGIC )) 

// COMMAND ----------

// MAGIC %scala
// MAGIC import org.apache.spark.sql.DataFrame
// MAGIC import org.apache.spark.sql.functions._
// MAGIC import org.apache.spark.sql.types._
// MAGIC  
// MAGIC def jsonToDataFrame(json: String, schema: StructType = null): DataFrame = {
// MAGIC   // SparkSessions are available with Spark 2.0+
// MAGIC   val reader = spark.read
// MAGIC   Option(schema).foreach(reader.schema)
// MAGIC   reader.json(sc.parallelize(Array(json)))
// MAGIC }
// MAGIC val actors = jsonToDataFrame("""
// MAGIC [
// MAGIC     {
// MAGIC       "imdb_title_id":"tt0000009",
// MAGIC       "ordering":1,
// MAGIC       "imdb_name_id":"nm0063086",
// MAGIC       "category":"actress",
// MAGIC       "job":"",
// MAGIC       "characters":"[Miss Geraldine Holbrook (Miss Jerry)]"
// MAGIC     },
// MAGIC     {
// MAGIC       "imdb_title_id":"tt0000009",
// MAGIC       "ordering":2,
// MAGIC       "imdb_name_id":"nm0183823",
// MAGIC       "category":"actor",
// MAGIC       "job":"",
// MAGIC       "characters":"[Mr. Hamilton]"
// MAGIC     },
// MAGIC     {
// MAGIC       "imdb_title_id":"tt0000009",
// MAGIC       "ordering":3,
// MAGIC       "imdb_name_id":"nm1309758",
// MAGIC       "category":"actor",
// MAGIC       "job":"",
// MAGIC       "characters":"[Chauncey Depew - the Director of the New York Central Railroad]"
// MAGIC     }
// MAGIC   ]
// MAGIC """, schemat)
// MAGIC 
// MAGIC display(actors)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Zadanie 3

// COMMAND ----------

// MAGIC %scala
// MAGIC import org.apache.spark.sql.functions._
// MAGIC 
// MAGIC val kolumny = Seq("timestamp","unix", "Date")
// MAGIC val dane = Seq(("2015-03-22T14:13:34", 1646641525847L,"May, 2021"),
// MAGIC                ("2015-03-22T15:03:18", 1646641557555L,"Mar, 2021"),
// MAGIC                ("2015-03-22T14:38:39", 1646641578622L,"Jan, 2021"))
// MAGIC 
// MAGIC var dataFrame = spark.createDataFrame(dane).toDF(kolumny:_*)
// MAGIC   .withColumn("current_date",current_date().as("current_date"))
// MAGIC   .withColumn("current_timestamp",current_timestamp().as("current_timestamp"))
// MAGIC display(dataFrame)

// COMMAND ----------

// MAGIC %scala
// MAGIC dataFrame.printSchema()

// COMMAND ----------

// MAGIC %scala
// MAGIC val nowyunix = dataFrame.select($"timestamp",unix_timestamp($"timestamp","yyyy-MM-dd'T'HH:mm:ss").cast("timestamp")).show()

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC val changed = dataFrame
// MAGIC   .withColumnRenamed("timestamp", "xxxx")
// MAGIC   .select( $"*", unix_timestamp($"xxxx", "yyyy-MM-dd'T'HH:mm:ss").cast("timestamp") )
// MAGIC   .withColumnRenamed("CAST(unix_timestamp(capturedAt, yyyy-MM-dd'T'HH:mm:ss) AS TIMESTAMP)", "xxxcast")
// MAGIC   .show()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Zadanie 4

// COMMAND ----------

// MAGIC %scala
// MAGIC spark.read.format("csv").option("mode", "PERMISSIVE").schema(schemat).load(filePath)
// MAGIC spark.read.format("csv").option("mode", "DROPMALFORMED").schema(schemat).load(filePath)
// MAGIC spark.read.format("csv").option("mode", "FAILFAST").schema(schemat).load(filePath)
// MAGIC spark.read.format("csv").option("mode", "FAILFAST").schema(schemat).option("badRecordsPath", "/temp/badRecordsPath").load(filePath)

// COMMAND ----------

// MAGIC %md
// MAGIC ##Zadanie 5

// COMMAND ----------

// MAGIC %scala
// MAGIC val outputPath="dbfs:/FileStore/tables/Files/actors.parquet"
// MAGIC actorsDf.write.parquet(outputPath)
// MAGIC spark.read.schema(schemat).parquet(outputPath)