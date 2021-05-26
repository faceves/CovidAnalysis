package com.revature.CovidAnalysis

import com.google.common.math.DoubleMath.mean
import jdk.internal.net.http.common.Utils.close
import loadpath.LoadPath
import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{array, avg, col, desc, explode, lit, struct}
import org.apache.spark.sql.types.{DecimalType, DoubleType, FloatType, IntegerType, StringType}

import java.text.SimpleDateFormat
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

    val covid_accum_DB = spark.read
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

    covid_accum_DB.show()
    covid_confirmed_US_DB.show()
    covid_confirmed_DB.show()
    covid_deaths_DB.show()
    covid_deaths_US_DB.show()
    covid_recovered_DB.show()

    var stateName = "New Hampshire"
    var date = "11/1/20"
    val stateDF = covid_confirmed_US_DB.filter('Province_State===stateName)
    changeOverTime(stateDF, date, 14).show
    growthFactor(stateDF, date).show
    stateDF.show
    //println(greatestGrowthFactor(stateDF))
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
    var df_mod = df.select(df("Combined_Key").as("Location"), df(startDate).cast(IntegerType)).orderBy(desc(startDate))
    val df_temp = df.select(df("Combined_Key").as("_n_"), df(dt).cast(IntegerType)).orderBy(desc(dt))
    df_mod = df_mod.join(df_temp, df_temp("_n_")===df_mod("Location"), "inner").drop("_n_")
    df_mod.withColumn("delta",df_mod(dt)-df_mod(startDate))
  }
  //only works when date is between 1/23/20-5/2/21
  def growthFactor(df: DataFrame, strDate: String) ={
    val format = new SimpleDateFormat("M/d/yy")
    val date = format.parse(strDate)
    val c = Calendar.getInstance()
    c.setTime(date)
    c.add(Calendar.DATE, -1)
    val dt = format.format(c.getTime())
    var df_mod = df.select(df("Combined_Key").as("_n_"), df(strDate).cast(IntegerType)).orderBy(desc(strDate))
    var df_temp = df.select(df("Combined_Key").as("Location"), df(dt).cast(IntegerType)).orderBy(desc(dt))
    df_mod = df_temp.join(df_mod, df_temp("Location")===df_mod("_n_"), "inner").drop("_n_")
    df_mod.withColumn("delta",(((df_mod(strDate)/df_mod(dt))-1)*100).cast(DecimalType(8, 2))).na.fill(0)
  }
  //super expensive and time consuming (should probably run this, save the output, and show the output in the demo as opposed to running it during the live demo)
  def greatestGrowthFactor(df: DataFrame) ={
    val format = new SimpleDateFormat("M/d/yy")
    val date = format.parse("1/23/20")
    val c = Calendar.getInstance()
    c.setTime(date)
    var dt = format.format(c.getTime())
    var greatestDate = ""
    var x = 0
    var tempDF = df
    var greatestMedianGrowthFactor = -1000000.0
    var medianGrowthFactor = 0.0
    for ( x <- 0 to 465){
      tempDF = growthFactor(df,dt)
      medianGrowthFactor = tempDF.select("delta").stat.approxQuantile("delta",Array(0.5),0.001).take(1).apply(0).toString().replaceAll("[^a-zA-Z0-9\\s\\:.-]", "").toDouble
      if (medianGrowthFactor > greatestMedianGrowthFactor) {
        greatestMedianGrowthFactor = medianGrowthFactor
        greatestDate = dt
      }
      c.add(Calendar.DATE, 1)
      dt = format.format(c.getTime())
    }
    tempDF = growthFactor(df,greatestDate)
    tempDF.show
    greatestMedianGrowthFactor
  }
}
