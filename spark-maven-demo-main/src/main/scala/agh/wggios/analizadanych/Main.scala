package agh.wggios.analizadanych


import agh.wggios.analizadanych.caseclass.Flights
import agh.wggios.analizadanych.datareader.DataReader
import agh.wggios.analizadanych.transformations.Transformation
import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[4]")
      .appName("Main")
      .getOrCreate();

    import spark.implicits._
    val df = new DataReader().readData(spark,"2013-summary.csv").as[Flights]
    df.filter(row => new Transformation(150).airport_filtering(row)).show()

  }
}