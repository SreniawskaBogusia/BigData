// Databricks notebook source
// MAGIC %md
// MAGIC ## Zadanie 1

// COMMAND ----------

// MAGIC %md 
// MAGIC Wykożystaj dane z bazy 'bidevtestserver.database.windows.net'
// MAGIC ||
// MAGIC |--|
// MAGIC |SalesLT.Customer|
// MAGIC |SalesLT.ProductModel|
// MAGIC |SalesLT.vProductModelCatalogDescription|
// MAGIC |SalesLT.ProductDescription|
// MAGIC |SalesLT.Product|
// MAGIC |SalesLT.ProductModelProductDescription|
// MAGIC |SalesLT.vProductAndDescription|
// MAGIC |SalesLT.ProductCategory|
// MAGIC |SalesLT.vGetAllCategories|
// MAGIC |SalesLT.Address|
// MAGIC |SalesLT.CustomerAddress|
// MAGIC |SalesLT.SalesOrderDetail|
// MAGIC |SalesLT.SalesOrderHeader|

// COMMAND ----------

//INFORMATION_SCHEMA.TABLES

val jdbcHostname = "bidevtestserver.database.windows.net"
val jdbcPort = 1433
val jdbcDatabase = "testdb"

val tabela = spark.read
  .format("jdbc")
  .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
  .option("user","sqladmin")
  .option("password","$3bFHs56&o123$")
  .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("query","SELECT * FROM INFORMATION_SCHEMA.TABLES")
  .load()
display(tabela)

// COMMAND ----------

// MAGIC %md
// MAGIC 1. Pobierz wszystkie tabele z schematu SalesLt i zapisz lokalnie bez modyfikacji w formacie delta

// COMMAND ----------

import org.apache.spark.sql.functions.col
val table_names= tabela.select(col("TABLE_NAME")).filter('TABLE_SCHEMA === "SalesLT").as[String].collect.toList
val i = List()
for ( i <- table_names){
  val tabela = spark.read
  .format("jdbc")
  .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
  .option("user","sqladmin")
  .option("password","$3bFHs56&o123$")
  .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("query",s"SELECT * FROM SalesLT.$i")
  .load()
  tabela.write.format("delta").mode("overwrite").saveAsTable(i)
}

// COMMAND ----------

// MAGIC %md
// MAGIC  Uzycie Nulls, fill, drop, replace, i agg
// MAGIC  * W każdej z tabel sprawdź ile jest nulls w rzędach i kolumnach
// MAGIC  * Użyj funkcji fill żeby dodać wartości nie występujące w kolumnach dla wszystkich tabel z null
// MAGIC  * Użyj funkcji drop żeby usunąć nulle, 
// MAGIC  * wybierz 3 dowolne funkcje agregujące i policz dla TaxAmt, Freight, [SalesLT].[SalesOrderHeader]
// MAGIC  * Użyj tabeli [SalesLT].[Product] i pogrupuj według ProductModelId, Color i ProductCategoryID i wylicz 3 wybrane funkcje agg() 
// MAGIC    - Użyj conajmniej dwóch overloded funkcji agregujących np z (Map)

// COMMAND ----------


import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col,when, count}

def countCols(columns:Array[String]):Array[Column]={
    columns.map(c=>{
      count(when(col(c).isNull,c)).alias(c)
    })
}

for( i <- table_names.map(x => x.toLowerCase())){
  val filePath = s"dbfs:/user/hive/warehouse/$i"
  val df = spark.read.format("delta")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)
print(i)
df.select(countCols(df.columns):_*).show()
  
}

// COMMAND ----------


for( i <- table_names.map(x => x.toLowerCase())){
  val df = spark.read.format("delta")
              .option("header","true")
              .option("inferSchema","true")
              .load(s"dbfs:/user/hive/warehouse/$i")
  print(i)
  df.na.fill("0", df.columns).show()
}


// COMMAND ----------

for( i <- table_names.map(x => x.toLowerCase())){
  val df = spark.read.format("delta")
              .option("header","true")
              .option("inferSchema","true")
              .load(s"dbfs:/user/hive/warehouse/$i")
  print(i)
  df.na.drop().show
}

// COMMAND ----------

import org.apache.spark.sql.functions._

val filePath = "dbfs:/user/hive/warehouse/salesorderheader"
val df = spark.read.format("delta")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)

df.agg(sum("TaxAmt")).show()
df.agg(sum("Freight")).show()
df.agg(avg("TaxAmt")).show()
df.agg(avg("Freight")).show()
df.agg(min("TaxAmt")).show()
df.agg(min("Freight")).show()


// COMMAND ----------

import org.apache.spark.sql.functions._

val filePath = s"dbfs:/user/hive/warehouse/product"
val df = spark.read.format("delta")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)

df.groupBy("ProductModelId", "Color", "ProductCategoryID").agg(min("StandardCost")).show()
df.groupBy("ProductModelId", "Color", "ProductCategoryID").agg(avg("ListPrice")).show()
df.groupBy("ProductModelId", "Color", "ProductCategoryID").agg(Map("StandardCost"->"min","ListPrice" -> "avg")).show()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Zadanie 2

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val squaredId = udf((i: Int) => i * i)
val dividedId = udf((d: Double) => d/2)
val lowerColor = udf((s: String) => s.toLowerCase)

df.select(squaredId(col("ProductId")) as "id_squared").show()
df.select(dividedId(col("ProductId")) as "id_divided").show()
df.select(lowerColor(col("Color")) as "lower_color").show()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Zadanie 3

// COMMAND ----------

import org.apache.spark.sql.functions._
val path = "dbfs:/FileStore/tables/Files/brzydki.json"
val jsonFile = spark.read.option("multiline","true").json(path)
jsonFile.printSchema()

val newJson=jsonFile.select("jobDetails.*")
newJson.printSchema()
