// Databricks notebook source
// MAGIC %md
// MAGIC ## Zadanie 2

// COMMAND ----------

// MAGIC %md
// MAGIC * Spark Jobs - widok wyświetla podsumowanie wszystkich zadań w aplikacji Spark. Zadania te są uruchamiane przez akcje, a nie transformacje
// MAGIC * Stages - pokazuje podsumowanie, które pokazuje bieżący stan wszystkich etapów wszystkich zadań w aplikacji Spark
// MAGIC * Storage - wyświetla w aplikacji utrwalone dyski RDD i ramki DataFrames, jeśli takie istnieją 
// MAGIC * Environment - wyświetla wartości różnych zmiennych środowiskowych i konfiguracyjnych
// MAGIC * Executors - wyświetla informacje o węzłach wykonawczych, które zostały utworzone dla aplikacji, w tym wykorzystanie pamięci i dysku oraz informacje o zadaniach
// MAGIC * SQL - wyświetla informacje, takie jak czas trwania, zadania w aplikacji Spark oraz plany fizyczne i logiczne dla zapytań wykonywane przez silnik SQL

// COMMAND ----------

// MAGIC %md
// MAGIC ## Zadanie 3

// COMMAND ----------

val filePath = "dbfs:/FileStore/tables/Files/names.csv"
val namesDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)

namesDf.explain()
namesDf.groupBy("divorces").count().explain()

// COMMAND ----------

// MAGIC %md
// MAGIC ##Zadanie 4

// COMMAND ----------

val user = "sqladmin"
val password = "$3bFHs56&o123$"
val jdbc = "jdbc:sqlserver://bidevtestserver.database.windows.net:1433;database=testdb"
val driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
 
 
val df = spark.read
  .format("jdbc")
  .option("url", jdbc)
  .option("user", user)
  .option("password", password)
  .option("driver", driver)
  .option("query", "SELECT * FROM information_schema.tables")
  .load()

display(df)