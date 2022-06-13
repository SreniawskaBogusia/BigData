// Databricks notebook source
// MAGIC %md Names.csv 
// MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
// MAGIC * Dodaj kolumnę w której wyliczysz wzrost w stopach (feet)
// MAGIC * Odpowiedz na pytanie jakie jest najpopularniesze imię?
// MAGIC * Dodaj kolumnę i policz wiek aktorów 
// MAGIC * Usuń kolumny (bio, death_details)
// MAGIC * Zmień nazwy kolumn - dodaj kapitalizaję i usuń _
// MAGIC * Posortuj dataframe po imieniu rosnąco

// COMMAND ----------

val filePath = "dbfs:/FileStore/tables/Files/names.csv"
val namesDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)
        
display(namesDf)

// COMMAND ----------


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val newDf = namesDf.withColumn("current_unix_timestamp",unix_timestamp())
                   .withColumn("height_ft", col("height")* 0.3937007874) // cm to ft
                   .drop("bio","deth_details") // usunięcie kolumn
                   .sort(asc("name")) // sortowanie rosnąco
                   .withColumn("current_date",current_date())
                   .withColumn("birth",to_date($"date_of_birth", "dd.MM.yyyy"))
                   .withColumn("death",to_date($"date_of_death", "dd.MM.yyyy"))
                   .withColumn("diffAge", 
                               when($"death".isNull ,round(months_between($"birth",$"current_date")/(-12))).otherwise(floor(months_between($"birth",$"death")/(-12))))
display(newDf)
      

// COMMAND ----------

//najpopularniejsze imie - John
val names = newDf.select(split(col("name")," ").as("nameArray"))
val firstNames = names.selectExpr("nameArray[0]").withColumnRenamed("nameArray[0]", "firstName")
val popularNames =  firstNames.groupBy("firstName").count().sort(desc("count"))
display(popularNames)

// COMMAND ----------

//zmiana nazwy kolumn
val renamedColumns = newDf.columns.map(x => x.split('_').map(_.capitalize).mkString(""))
val newCol= newDf.toDF(renamedColumns: _*)
display(newCol)

// COMMAND ----------

// MAGIC %md Movies.csv
// MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
// MAGIC * Dodaj kolumnę która wylicza ile lat upłynęło od publikacji filmu
// MAGIC * Dodaj kolumnę która pokaże budżet filmu jako wartość numeryczną, (trzeba usunac znaki walut)
// MAGIC * Usuń wiersze z dataframe gdzie wartości są null

// COMMAND ----------

val filePath = "dbfs:/FileStore/tables/Files/movies.csv"
val moviesDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)

display(moviesDf)

// COMMAND ----------

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
val newDf = moviesDf.withColumn("current_unix_timestamp",unix_timestamp()) //dodanie czasu
                  .withColumn("current_time",current_date())
                  .withColumn("publishing_year",to_date($"year", "yyyy"))
                  .withColumn("time", round(months_between($"publishing_year",$"current_time")/(-12)))
                  .withColumn("new_budget",regexp_replace($"budget","\\D",""))
                  .na.drop()
display(newDf)

// COMMAND ----------

// MAGIC %md ratings.csv
// MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
// MAGIC * Dla każdego z poniższych wyliczeń nie bierz pod uwagę `nulls` 
// MAGIC * Dodaj nowe kolumny i policz mean i median dla wartości głosów (1 d 10)
// MAGIC * Dla każdej wartości mean i median policz jaka jest różnica między weighted_average_vote
// MAGIC * Kto daje lepsze oceny chłopcy czy dziewczyny dla całego setu
// MAGIC * Dla jednej z kolumn zmień typ danych do `long` 

// COMMAND ----------

val filePath = "dbfs:/FileStore/tables/Files/ratings.csv"
val ratingsDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)

display(ratingsDf)

// COMMAND ----------

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

val newDf = ratingsDf.withColumn("current_unix_timestamp",unix_timestamp())
                     .na.drop()
                     .withColumn("total_votes", col("total_votes").cast(LongType))
display(newDf)