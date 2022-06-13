// Databricks notebook source
// MAGIC %md
// MAGIC ## Jak działa partycjonowanie
// MAGIC 
// MAGIC 1. Rozpocznij z 8 partycjami.
// MAGIC 2. Uruchom kod.
// MAGIC 3. Otwórz **Spark UI**
// MAGIC 4. Sprawdź drugi job (czy są jakieś różnice pomięczy drugim)
// MAGIC 5. Sprawdź **Event Timeline**
// MAGIC 6. Sprawdzaj czas wykonania.
// MAGIC   * Uruchom kilka razy rzeby sprawdzić średni czas wykonania.
// MAGIC 
// MAGIC Powtórz z inną liczbą partycji
// MAGIC * 1 partycja
// MAGIC * 7 partycja
// MAGIC * 9 partycja
// MAGIC * 16 partycja
// MAGIC * 24 partycja
// MAGIC * 96 partycja
// MAGIC * 200 partycja
// MAGIC * 4000 partycja
// MAGIC 
// MAGIC Zastąp `repartition(n)` z `coalesce(n)` używając:
// MAGIC * 6 partycji
// MAGIC * 5 partycji
// MAGIC * 4 partycji
// MAGIC * 3 partycji
// MAGIC * 2 partycji
// MAGIC * 1 partycji
// MAGIC 
// MAGIC ** *Note:* ** *Dane muszą być wystarczająco duże żeby zaobserwować duże różnice z małymi partycjami.*<br/>* To co możesz sprawdzić jak zachowują się małe dane z dużą ilośćia partycji.*

// COMMAND ----------

// val slots = sc.defaultParallelism
spark.conf.get("spark.sql.shuffle.partitions")

// COMMAND ----------

spark.catalog.clearCache()

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val schema = StructType(
  List(
    StructField("timestamp", StringType, false),
    StructField("site", StringType, false),
    StructField("requests", IntegerType, false)
  )
)

val fileName = "dbfs:/databricks-datasets/wikipedia-datasets/data-001/pageviews/raw"

var df = spark.read
  .option("header", "true")
  .option("sep", "\t")
  .schema(schema)
  .csv(fileName)

val path = "dbfs:/wikipedia.parquet"

df.write.mode("overwrite").parquet(path)

df.explain
df.count()

// COMMAND ----------

df.rdd.getNumPartitions


// COMMAND ----------

df = spark.read
  .parquet(path)
  .repartition(8)
  .groupBy("site")
  .sum()


// COMMAND ----------

df = spark.read
  .parquet(path)
  .repartition(1)  
  .groupBy("site")
  .sum()


// COMMAND ----------

df = spark.read
  .parquet(path)
  .repartition(7)
  .groupBy("site")
  .sum()


// COMMAND ----------

df = spark.read
  .parquet(path)
  .repartition(9)
  .groupBy("site")
  .sum()


// COMMAND ----------

df = spark.read
  .parquet(path)
  .repartition(16)
  .groupBy("site")
  .sum()


// COMMAND ----------

df = spark.read
  .parquet(path)
  .repartition(24)
  .groupBy("site")
  .sum()


// COMMAND ----------

df = spark.read
  .parquet(path)
  .repartition(96)
  .groupBy("site")
  .sum()


// COMMAND ----------

df = spark.read
  .parquet(path)
  .repartition(200)
  .groupBy("site")
  .sum()


// COMMAND ----------

df = spark.read
  .parquet(path)
  .repartition(4000)
  .groupBy("site")
  .sum()


// COMMAND ----------

df = spark.read
  .parquet(path)
  .coalesce(6)
  .groupBy("site")
  .sum()


// COMMAND ----------

df = spark.read
  .parquet(path)
  .coalesce(4)
  .groupBy("site")
  .sum()

// COMMAND ----------

df = spark.read
  .parquet(path)
  .coalesce(3)
  .groupBy("site")
  .sum()


// COMMAND ----------

df = spark.read
  .parquet(path)
  .coalesce(2)
  .groupBy("site")
  .sum()


// COMMAND ----------

df = spark.read
  .parquet(path)
  .coalesce(1)
  .groupBy("site")
  .sum()


// COMMAND ----------

df.rdd.getNumPartitions

// COMMAND ----------

// MAGIC %md
// MAGIC ### POKA YOKE

// COMMAND ----------

import java.nio.file.{Paths, Files}

def path_exists(path:String):Boolean = { 
    val res = dbutils.fs.ls(path)
    if( res.length > 0 ){
      return true
    }
    return false
}



def count_null(colName : String, df :DataFrame) : Long ={
  if(!df.columns.contains(colName))
  {
      println("There is no such column")
      return 1
  } 
  return df.filter(col(colName).isNull).count()
}



def is_number(str:String): Boolean = {
    val regex = "\\d+\\.?\\d+"
    return str.matches(regex)
}


def is_empty(str:String): Boolean = {
    return str.isEmpty
}


def table_exists(table: String) =
  sqlContext.tableNames.contains(table)


import org.apache.spark.sql.DataFrame

def get_max_value(colName: String, df: DataFrame): Any={
  if(!df.columns.contains(colName)){
      println("There is no such column")
      return 0
  }
  if(df.schema(colName).dataType.typeName == "string"){
    println("This is string column")
    return 0
  }
    return df.agg(max(colName)).first.get(0)
  
}