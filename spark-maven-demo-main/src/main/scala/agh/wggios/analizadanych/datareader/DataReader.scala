package agh.wggios.analizadanych.datareader

import org.apache.spark.sql.{DataFrame, SparkSession}

class DataReader {
  def readData(spark: SparkSession, path: String): DataFrame ={
    spark.read.format("csv").option("header", value = true).option("inferSchema", "true").load(path)
  }
}
