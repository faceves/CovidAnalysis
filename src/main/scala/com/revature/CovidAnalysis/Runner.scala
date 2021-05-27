package com.revature.CovidAnalysis

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import loadpath.LoadPath
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.sources.And
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{array, col, desc, explode, lit, struct}
import org.apache.spark.sql.types.{DateType, IntegerType, TimestampType}
import org.apache.spark.sql.{Encoder, Encoders}

import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Date}
import scala.reflect.internal.util.NoPosition.show


object Runner {

  def main(args: Array[String]) : Unit = {
    val spark = SparkSession
      .builder()
      .appName("Covid Analysis")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    Logger.getLogger("org").setLevel(Level.ERROR)

    //implicit val enc: Encoder[CovidAccum] = Encoders.product[CovidAccum]

    val covid_accum_DB = spark.read
      .option("header", true)
      .option("delimiter",",")
      .format("csv")
      .load(LoadPath.hdfs_path + "covid_19_data.csv")
      .toDF()


    //implicit val enc: Encoder[CovidAccum] = Encoders.product[CovidAccum]

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

    val filteredDF =
      covid_accum_DB.where($"Confirmed">0)


    /**
    covid_accum_DB.show()
    covid_confirmed_US_DB.show()
    covid_confirmed_DB.show()
    covid_deaths_DB.show()
    covid_deaths_US_DB.show()
    covid_recovered_DB.show()
    **/

    val dt = firstOccurrence(covid_accum_DB,"Deaths","Province/State")
    dt.show()

    spark.close()
  }

  //do not use with covid_accum_DB
  def changeOverTime(df: DataFrame, startDate: String, numOfDays: Int) ={
    val format = new SimpleDateFormat("M/d/yy")
    val date = format.parse(startDate)
    val c = Calendar.getInstance()
    c.setTime(date)
    c.add(Calendar.DATE, numOfDays)
    val dt = format.format(c.getTime())
    var df_mod = df.select(df("Combined_Key"), df(startDate).cast(IntegerType)).orderBy(desc(startDate))
    val df_temp = df.select(df("Combined_Key").as("_n_"), df(dt).cast(IntegerType)).orderBy(desc(dt))
    df_mod = df_mod.join(df_temp, df_temp("_n_")===df_mod("Combined_Key"), "inner").drop("_n_")
    df_mod.withColumn("delta",df_mod(dt)-df_mod(startDate))
  }


  /**
   * @param df, occurTypeCol = "Deaths", occurByCol = "Country/Region"
   * */

  def firstOccurrence(df: DataFrame, occurTypeCol: String, occurByCol: String): DataFrame = {

    val filteredDF =
      df.where(s"$occurTypeCol > 0")
      .withColumn("Date",to_date(df("ObservationDate"), "MM/dd/yy"))

    val window = Window.partitionBy(occurByCol).orderBy("Date")
    val firstOccurDF =
        filteredDF.withColumn("rowNum", row_number().over(window))
          .where("rowNum == 1" )
          .drop("Date","rowNum")
    firstOccurDF

  }

}
