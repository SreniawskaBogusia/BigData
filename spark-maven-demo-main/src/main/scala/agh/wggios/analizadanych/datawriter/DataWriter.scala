package agh.wggios.analizadanych.datawriter

import org.apache.spark.sql.DataFrame

class DataWriter {
  def writeData(dataframe: DataFrame, path: String): Unit = {
    dataframe.write.format("csv").save(path)
  }
}
