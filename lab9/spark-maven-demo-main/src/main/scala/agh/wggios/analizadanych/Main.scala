package agh.wggios.analizadanych

import agh.wggios.analizadanych.datareader.DataReader

object Main extends SparkSessionProvider {
LoggingUtils.setupLogging()
  def main(args: Array[String]): Unit = {

    import spark.implicits._
    logInfo("odpalam")
    val df = new DataReader().read_csv(args(0))
    df.show()

  }
}

import org.apache.spark.sql.{SparkSession, DataFrame}

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Spark Maven Demo")
      .master("local[*]")
      .getOrCreate()

    val df = readData(spark, "data/input.txt")
    val transformedDf = transformData(df)
    writeData(transformedDf, "data/output")
  }

  def readData(spark: SparkSession, path: String): DataFrame = {
    spark.read.text(path)
  }

  def transformData(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._
    df.withColumnRenamed("value", "line")
      .filter($"line".contains("Spark"))
  }

  def writeData(df: DataFrame, path: String): Unit = {
    df.write.mode("overwrite").text(path)
  }
}

