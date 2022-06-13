// Databricks notebook source
// MAGIC %md
// MAGIC ## Zadanie 1
// MAGIC 
// MAGIC Indeksowanaie w Hive jest stosunkowo nową funkcją. Przechowywanie indeksów wymaga miejsca na dysku, a tworzenie indeksu wiąże się z kosztami. 
// MAGIC 
// MAGIC Zapytanie „EXPLAIN” musi być zaznaczone, aby ocenić korzyści poprzez plan wykonania zapytania. Aby sprawdzić, czy indeksowanie jest potrzebne najlepiej będzie porównać koszty wykonania indeksów z korzyściami. 

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Zadanie 3

// COMMAND ----------

spark.catalog.listDatabases()

// COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS db")

// COMMAND ----------

val filePath = "dbfs:/FileStore/tables/Files/names.csv"
val df = spark.read.format("csv")
    .option("header","true")
    .option("inferSchema","true")
    .load(filePath)

df.write.mode("overwrite").saveAsTable("db.names")

// COMMAND ----------

val filePath = "dbfs:/FileStore/tables/Files/actors.csv"
val df = spark.read.format("csv")
    .option("header","true")
    .option("inferSchema","true")
    .load(filePath)

df.write.mode("overwrite").saveAsTable("db.actors")

// COMMAND ----------

spark.catalog.listTables("db").show()

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

def drop_tables(database: String){
  
  val tables = spark.catalog.listTables(s"$database")
  val names = tables.select("name").as[String].collect.toList
  var i = List()
  for( i <- names){
    spark.sql(s"DELETE FROM $database.$i")
  }
  
}

drop_tables("db")

// COMMAND ----------

spark.sql("SELECT * FROM db.names").show()

// COMMAND ----------

spark.sql("SELECT * FROM db.actors").show()