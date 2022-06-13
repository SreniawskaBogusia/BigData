// Databricks notebook source
// MAGIC %md
// MAGIC ## Zadanie 1

// COMMAND ----------

spark.sql("create database if not exists Sample")


// COMMAND ----------

val dataTransactions = Seq(( 1, "2011-01-01", 500),
( 1, "2011-01-15", 50),
( 1, "2011-01-22", 250),
( 1, "2011-01-24", 75),
( 1, "2011-01-26", 125),
( 1, "2011-01-28", 175),
( 2, "2011-01-01", 500),
( 2, "2011-01-15", 50),
( 2, "2011-01-22", 25),
( 2, "2011-01-23", 125),
( 2, "2011-01-26", 200),
( 2, "2011-01-29", 250),
( 3, "2011-01-01", 500),
( 3, "2011-01-15", 50 ),
( 3, "2011-01-22", 5000),
( 3, "2011-01-25", 550),
( 3, "2011-01-27", 95 ),
( 3, "2011-01-30", 2500))

val dataLogical = Seq((1,"George", 800),
(2,"Sam", 950),
(3,"Diane", 1100),
(4,"Nicholas", 1250),
(5,"Samuel", 1250),
(6,"Patricia", 1300),
(7,"Brian", 1500),
(8,"Thomas", 1600),
(9,"Fran", 2450),
(10,"Debbie", 2850),
(11,"Mark", 2975),
(12,"James", 3000),
(13,"Cynthia", 3000),
(14,"Christopher", 5000))


// COMMAND ----------

val rddTransactions = spark.sparkContext.parallelize(dataTransactions)
val Transactions = rddTransactions.toDF("AccountId", "TranDate", "TranAmt")
Transactions.createOrReplaceTempView("Transactions")
spark.sql("create table if not exists Sample.transactionsDf as select * from Transactions")
spark.sql("select * from Sample.transactionsDf").show()

// COMMAND ----------

val rddLogical = spark.sparkContext.parallelize(dataLogical)
val Logical = rddLogical.toDF("RowID","FName", "Salary")
Logical.createOrReplaceTempView("Logical")
spark.sql("create table if not exists Sample.logicalDf as select * from Logical")
spark.sql("select * from Sample.logicalDf").show()

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

val windowRunTotalAmt  = Window.partitionBy("AccountId").orderBy("TranDate")
Transactions.withColumn("RunTotalAmt",sum("TranAmt").over(windowRunTotalAmt)).orderBy("AccountId","TranDate").show()

// COMMAND ----------


val windowSpec  = Window.partitionBy("AccountId").orderBy("TranDate")
Transactions.withColumn("RunAvg",avg("TranAmt").over(windowSpec)).orderBy("AccountId","TranDate")
            .withColumn("RunTranQty",count("*").over(windowSpec)).orderBy("AccountId","TranDate")
            .withColumn("RunSmallAmt",min("TranAmt").over(windowSpec)).orderBy("AccountId","TranDate")
            .withColumn("RunLargeAmt",max("TranAmt").over(windowSpec)).orderBy("AccountId","TranDate")
            .withColumn("RunTotalAmt",sum("TranAmt").over(windowSpec)).orderBy("AccountId","TranDate").show()

// COMMAND ----------

val windowSpec1  = Window.partitionBy("AccountId").orderBy("TranDate").rowsBetween(-2,Window.currentRow)
val windowSpec2  = Window.partitionBy("AccountId").orderBy("TranDate")

Transactions.withColumn("RN",row_number().over(windowSpec2))
            .withColumn("SlideAvg",avg("TranAmt").over(windowSpec1))
            .withColumn("SlideQty",count("*").over(windowSpec1))
            .withColumn("SlideMin",min("TranAmt").over(windowSpec1))
            .withColumn("SlideMax",max("TranAmt").over(windowSpec1))
            .withColumn("SlideTotal",sum("TranAmt").over(windowSpec1))
            .orderBy("AccountId","TranDate","RN")
.show()


// COMMAND ----------

val windowSpec1 = Window.orderBy("Salary").rowsBetween(Window.unboundedPreceding, Window.currentRow)
val windowSpec2 = Window.orderBy("Salary").rangeBetween(Window.unboundedPreceding, Window.currentRow)

Logical.withColumn("SumByRows",sum("Salary").over(windowSpec1))
       .withColumn("SumByRange",sum("Salary").over(windowSpec2))
       .orderBy("RowID")
.show()


// COMMAND ----------

val windowSpec = Window.partitionBy("AccountId").orderBy("TranDate")
Transactions.withColumn("RN",row_number().over(windowSpec)).orderBy("AccountId").show(10)

// COMMAND ----------

// MAGIC %md 
// MAGIC ## Zadanie 2

// COMMAND ----------

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

val windowSpec = Window.orderBy("Salary")
val windowSpecRow = Window.orderBy("Salary").rowsBetween(-2, Window.currentRow)
val windowSpecRange = Window.orderBy("Salary").rangeBetween(-2, Window.currentRow)

Logical.withColumn("lead",lead(col("Salary"),1).over(windowSpec))
       .withColumn("lag",lag(col("Salary"),1).over(windowSpec))
       .withColumn("firstValueByRow",first("Salary").over(windowSpecRow))
       .withColumn("firstValueByRange",first("Salary").over(windowSpecRange))
       .withColumn("lastValueByRow",last("Salary").over(windowSpecRow))
       .withColumn("lastValueByRange",last("Salary").over(windowSpecRange))
       .withColumn("rowNumber",row_number().over(windowSpec))
       .withColumn("denseRank",dense_rank().over(windowSpec))
.show()


// COMMAND ----------

// MAGIC %md
// MAGIC ## Zadanie 3

// COMMAND ----------

val leftSemiJoin = Logical.join(Transactions,Logical("RowID") ===  Transactions("AccountId"),"leftsemi")
leftSemiJoin.explain()
leftSemiJoin.show()

// COMMAND ----------

val leftAntiJoin = Logical.join(Transactions,Logical("RowID") ===  Transactions("AccountId"),"leftanti")
leftAntiJoin.explain()
leftAntiJoin.show()

// COMMAND ----------

// MAGIC %md 
// MAGIC ## Zadanie 4

// COMMAND ----------

val resultDf = Logical.join(Transactions,Logical("RowID") ===  Transactions("AccountId"))
resultDf.show()


// COMMAND ----------

resultDf.dropDuplicates("TranAmt").show()

// COMMAND ----------

//resultDf.distinct().show()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Zadanie 5

// COMMAND ----------

val broadcastJoin = Logical.join(broadcast(Transactions),Logical("RowID") ===  Transactions("AccountId"))
broadcastJoin.explain()
broadcastJoin.show()


// COMMAND ----------

