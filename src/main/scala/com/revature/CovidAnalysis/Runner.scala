package com.revature.CovidAnalysis

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import loadpath.LoadPath
import org.apache.spark.sql.functions.{asc, desc, expr}
import org.apache.spark.sql.types.IntegerType

object Runner {

  def main(args: Array[String]) : Unit = {
    val spark = SparkSession
      .builder()
      .appName("Covid Analysis")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    Logger.getLogger("org").setLevel(Level.ERROR)

    val covid_analysis_DB = spark.read
      .option("header", true)
      .option("delimiter",",")
      .format("csv")
      .load(LoadPath.hdfs_path + "covid_19_data.csv")
      .toDF()

    val covid_confirmed_DB = spark.read
      .option("header", true)
      .option("delimiter",",")
      .format("csv")
      .load(LoadPath.hdfs_path + "time_series_covid_19_confirmed.csv")
      .toDF()

    val covid_confirmed_US_DB = spark.read
      .option("header", true)
      .option("delimiter",",")
      .format("csv")
      .load(LoadPath.hdfs_path + "time_series_covid_19_confirmed_US.csv")
      .toDF()

    val covid_deaths_DB = spark.read
      .option("header", true)
      .option("delimiter",",")
      .format("csv")
      .load(LoadPath.hdfs_path + "time_series_covid_19_deaths.csv")
      .toDF()

    val covid_deaths_US_DB = spark.read
      .option("header", true)
      .option("delimiter",",")
      .format("csv")
      .load(LoadPath.hdfs_path + "time_series_covid_19_deaths_US.csv")
      .toDF()

    val covid_recovered_DB = spark.read
      .option("header", true)
      .option("delimiter",",")
      .format("csv")
      .load(LoadPath.hdfs_path + "time_series_covid_19_recovered.csv")
      .toDF()

    //covid_analysis_DB.show()
    //covid_confirmed_US_DB.show()
    //covid_confirmed_DB.show()
    //covid_deaths_DB.show()
    //covid_deaths_US_DB.show()
    //covid_recovered_DB.show()

    val summaryAnalysis = covid_analysis_DB.summary()


    //val cov19dataX = covid_analysis_DB.select("Province/State", "Country/Region", "Deaths").orderBy("Country/Region").show()

    //val russianConfirmation = covid_analysis_DB.filter($"Country/Region" === "Russia").select($"Country/Region", $"Deaths".cast(IntegerType)).groupBy("Country/Region").sum("Deaths")
    val trueTotalDeaths = covid_analysis_DB.select($"Country/Region", $"Deaths".cast(IntegerType)).groupBy("Country/Region").sum("Deaths") //tally up Casualties by country
    val trueTotalConfirmed = covid_analysis_DB.select($"Country/Region", $"Confirmed".cast(IntegerType)).groupBy("Country/Region").sum("Confirmed")//tally up Confirmations by country
    val trueTotalRecovered = covid_analysis_DB.select($"Country/Region", $"Recovered".cast(IntegerType)).groupBy("Country/Region").sum("Recovered") //tally up Recoveries by country

    val joina = trueTotalDeaths.withColumnRenamed("sum(Deaths)","Deaths").join(trueTotalRecovered,Seq("Country/Region"), "Left").withColumnRenamed("sum(Recovered)", "Recovered") //Unite Deaths and Recoveries
    val joinb = joina.join(trueTotalConfirmed, Seq("Country/Region"), "Left").withColumnRenamed("sum(Confirmed)","Confirmed")   //Unite Deaths, Recoveries and Confirmations

    val ratio = joinb.withColumn("Decimal Deaths", joinb.col("Deaths").cast(IntegerType)/joinb.col("Confirmed").cast(IntegerType))
      .withColumn("Decimal Recoveries", joinb.col("Recovered").cast(IntegerType)/joinb.col("Confirmed").cast(IntegerType))
      .orderBy(desc("Confirmed"))

    joinb.show()
    ratio.show()

    spark.close()
  }



}
