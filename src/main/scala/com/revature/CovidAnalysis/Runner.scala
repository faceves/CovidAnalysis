package com.revature.CovidAnalysis

import com.google.common.math.DoubleMath.mean
import jdk.internal.net.http.common.Utils.close
import loadpath.LoadPath
import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{array, avg, col, desc, explode, lit, max, min, struct}
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

//    covid_accum_DB.show()
//    covid_confirmed_US_DB.show()
//    covid_confirmed_DB.show()
//    covid_deaths_DB.show()
//    covid_deaths_US_DB.show()
//    covid_recovered_DB.show()

    //var stateName = "New Hampshire"
    //var date = "11/28/20"
    //val stateDF = covid_confirmed_US_DB.filter('Province_State===stateName)
    //changeOverTime(stateDF, date, 14).show
    //growthFactor(stateDF, date).show
    //stateDF.show
    //println(greatestMedianGrowthFactor(stateDF, stateName))

    val stateDF = covid_confirmed_US_DB.select(covid_confirmed_US_DB("Province_State")).distinct()
    val stateList = stateDF.collect.map(_(0)).toList
    println(stateList)
    //stateList.foreach(f=>allGrowthFactorDataPoints(covid_confirmed_US_DB.filter('Province_State===f.toString).na.fill("").filter('Admin2=!="Unassigned"),"USA", f.toString,"Confirmed Cases"))

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
    val df_temp = df.select(df("Combined_Key").as("Location"), df(dt).cast(IntegerType)).orderBy(desc(dt))
    df_mod = df_temp.join(df_mod, df_temp("Location")===df_mod("_n_"), "inner").drop("_n_")
    df_mod.withColumn("delta",(((df_mod(strDate)/df_mod(dt))-1)*100).cast(DecimalType(8, 2))).na.fill(0)
  }
  //DO NOT RUN THESE FUNCTIONS
  /*super expensive and time consuming
  def greatestMedianGrowthFactor(df: DataFrame, Country:String,Province:String, dataName:String) ={
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
    tempDF.coalesce(1).write.option("header", "true").csv("output/"+Country+"/"+Province+"/"+dataName+"/greatestMedGF.csv")
    greatestMedianGrowthFactor
  }
  def greatestAvgGrowthFactor(df: DataFrame, Country:String,Province:String, dataName:String) ={
    val format = new SimpleDateFormat("M/d/yy")
    val date = format.parse("1/23/20")
    val c = Calendar.getInstance()
    c.setTime(date)
    var dt = format.format(c.getTime())
    var greatestDate = ""
    var x = 0
    var tempDF = df
    var greatestAvgGrowthFactor = -1000000.0
    var avgGrowthFactor = 0.0
    for ( x <- 0 to 465){
      tempDF = growthFactor(df,dt)
      avgGrowthFactor = tempDF.select(avg("delta")).take(1).apply(0).toString().replaceAll("[^a-zA-Z0-9\\s\\:.-]", "").toDouble
      if (avgGrowthFactor > greatestAvgGrowthFactor) {
        greatestAvgGrowthFactor = avgGrowthFactor
        greatestDate = dt
      }
      c.add(Calendar.DATE, 1)
      dt = format.format(c.getTime())
    }
    tempDF = growthFactor(df,greatestDate)
    tempDF.coalesce(1).write.csv("output/"+Country+"/"+Province+"/"+dataName+"/greatestAvgGF.csv")
    greatestAvgGrowthFactor
  }
  def leastMedianGrowthFactor(df: DataFrame, Country:String,Province:String, dataName:String) ={
    val format = new SimpleDateFormat("M/d/yy")
    val date = format.parse("1/23/20")
    val c = Calendar.getInstance()
    c.setTime(date)
    var dt = format.format(c.getTime())
    var leastDate = ""
    var x = 0
    var tempDF = df
    var leastMedianGrowthFactor = 1000000.0
    var medianGrowthFactor = 0.0
    for ( x <- 0 to 465){
      tempDF = growthFactor(df,dt)
      medianGrowthFactor = tempDF.select("delta").stat.approxQuantile("delta",Array(0.5),0.001).take(1).apply(0).toString().replaceAll("[^a-zA-Z0-9\\s\\:.-]", "").toDouble
      if (medianGrowthFactor < leastMedianGrowthFactor) {
        leastMedianGrowthFactor = medianGrowthFactor
        leastDate = dt
      }
      c.add(Calendar.DATE, 1)
      dt = format.format(c.getTime())
    }
    tempDF = growthFactor(df,leastDate)
    tempDF.coalesce(1).write.csv("output/"+Country+"/"+Province+"/"+dataName+"/leastMedGF.csv")
    leastMedianGrowthFactor
  }
  def leastAvgGrowthFactor(df: DataFrame, Country:String,Province:String, dataName:String) ={
    val format = new SimpleDateFormat("M/d/yy")
    val date = format.parse("1/23/20")
    val c = Calendar.getInstance()
    c.setTime(date)
    var dt = format.format(c.getTime())
    var leastDate = ""
    var x = 0
    var tempDF = df
    var leastAvgGrowthFactor = 1000000.0
    var avgGrowthFactor = 0.0
    for ( x <- 0 to 465){
      tempDF = growthFactor(df,dt)
      avgGrowthFactor = tempDF.select(avg("delta")).take(1).apply(0).toString().replaceAll("[^a-zA-Z0-9\\s\\:.-]", "").toDouble
      if (avgGrowthFactor < leastAvgGrowthFactor) {
        leastAvgGrowthFactor = avgGrowthFactor
        leastDate = dt
      }
      c.add(Calendar.DATE, 1)
      dt = format.format(c.getTime())
    }
    tempDF = growthFactor(df,leastDate)
    tempDF.coalesce(1).write.csv("output/"+Country+"/"+Province+"/"+dataName+"/leastAvgGF.csv")
    leastAvgGrowthFactor
  }
  def maxGrowthFactor(df: DataFrame, Country:String,Province:String, dataName:String) ={
    val format = new SimpleDateFormat("M/d/yy")
    val date = format.parse("1/23/20")
    val c = Calendar.getInstance()
    c.setTime(date)
    var dt = format.format(c.getTime())
    var leastDate = ""
    var x = 0
    var tempDF = df
    var maxGrowthFactor = -1000000.0
    var gFactor = 0.0
    for ( x <- 0 to 465){
      tempDF = growthFactor(df,dt)
      gFactor = tempDF.select(max("delta")).take(1).apply(0).toString().replaceAll("[^a-zA-Z0-9\\s\\:.-]", "").toDouble
      if (gFactor > maxGrowthFactor) {
        maxGrowthFactor = gFactor
        leastDate = dt
      }
      c.add(Calendar.DATE, 1)
      dt = format.format(c.getTime())
    }
    tempDF = growthFactor(df,leastDate)
    tempDF.coalesce(1).write.csv("output/"+Country+"/"+Province+"/"+dataName+"/maxGF.csv")
    maxGrowthFactor
  }
  def minGrowthFactor(df: DataFrame, Country:String,Province:String, dataName:String) ={
    val format = new SimpleDateFormat("M/d/yy")
    val date = format.parse("1/23/20")
    val c = Calendar.getInstance()
    c.setTime(date)
    var dt = format.format(c.getTime())
    var leastDate = ""
    var x = 0
    var tempDF = df
    var minGrowthFactor = 1000000.0
    var gFactor = 0.0
    for ( x <- 0 to 465){
      tempDF = growthFactor(df,dt)
      gFactor = tempDF.select(max("delta")).take(1).apply(0).toString().replaceAll("[^a-zA-Z0-9\\s\\:.-]", "").toDouble
      if (gFactor < minGrowthFactor) {
        minGrowthFactor = gFactor
        leastDate = dt
      }
      c.add(Calendar.DATE, 1)
      dt = format.format(c.getTime())
    }
    tempDF = growthFactor(df,leastDate)
    tempDF.coalesce(1).write.csv("output/"+Country+"/"+Province+"/"+dataName+"/maxGF.csv")
    minGrowthFactor
  }

  def allGrowthFactorDataPoints(df: DataFrame, Country:String,Province:String, dataName:String) ={
    val format = new SimpleDateFormat("M/d/yy")
    val date = format.parse("1/23/20")
    val c = Calendar.getInstance()
    c.setTime(date)
    var dt = format.format(c.getTime())
    var greatestMedDate = ""
    var x = 0
    var tempDF = df
    var greatestMedianGrowthFactor = -1000000.0
    var medianGrowthFactor = 0.0

    var greatestAvgDate = ""
    var greatestAvgGrowthFactor = -1000000.0
    var avgGrowthFactor = 0.0

    var leastMedDate = ""
    var leastMedianGrowthFactor = 1000000.0

    var leastAvgDate = ""
    var leastAvgGrowthFactor = 1000000.0

    var maxGrowthDate = ""
    var maxGrowthFactor = -1000000.0
    var gFactor = 0.0

    var minGrowthDate = ""
    var minGrowthFactor = 1000000.0

    for ( x <- 0 to 465){
      tempDF = growthFactor(df,dt)
      medianGrowthFactor = tempDF.select("delta").stat.approxQuantile("delta",Array(0.5),0.001).take(1).apply(0).toString().replaceAll("[^a-zA-Z0-9\\s\\:.-]", "").toDouble
      if (medianGrowthFactor > greatestMedianGrowthFactor) {
        greatestMedianGrowthFactor = medianGrowthFactor
        greatestMedDate = dt
      }

      avgGrowthFactor = tempDF.select(avg("delta")).take(1).apply(0).toString().replaceAll("[^a-zA-Z0-9\\s\\:.-]", "").toDouble
      if (avgGrowthFactor > greatestAvgGrowthFactor) {
        greatestAvgGrowthFactor = avgGrowthFactor
        greatestAvgDate = dt
      }

      //medianGrowthFactor = tempDF.select("delta").stat.approxQuantile("delta",Array(0.5),0.001).take(1).apply(0).toString().replaceAll("[^a-zA-Z0-9\\s\\:.-]", "").toDouble
      if (medianGrowthFactor < leastMedianGrowthFactor) {
        leastMedianGrowthFactor = medianGrowthFactor
        leastMedDate = dt
      }

      //avgGrowthFactor = tempDF.select(avg("delta")).take(1).apply(0).toString().replaceAll("[^a-zA-Z0-9\\s\\:.-]", "").toDouble
      if (avgGrowthFactor < leastAvgGrowthFactor) {
        leastAvgGrowthFactor = avgGrowthFactor
        leastAvgDate = dt
      }

      gFactor = tempDF.select(max("delta")).take(1).apply(0).toString().replaceAll("[^a-zA-Z0-9\\s\\:.-]", "").toDouble
      if (gFactor > maxGrowthFactor) {
        maxGrowthFactor = gFactor
        maxGrowthDate = dt
      }

      gFactor = tempDF.select(min("delta")).take(1).apply(0).toString().replaceAll("[^a-zA-Z0-9\\s\\:.-]", "").toDouble
      if (gFactor < minGrowthFactor) {
        minGrowthFactor = gFactor
        minGrowthDate = dt
      }
      c.add(Calendar.DATE, 1)
      dt = format.format(c.getTime())
    }

    tempDF = growthFactor(df,greatestMedDate)
    tempDF.coalesce(1).write.option("header", "true").csv("output/"+Country+"/"+Province+"/"+dataName+"/greatestMedGF.csv")

    tempDF = growthFactor(df,greatestAvgDate)
    tempDF.coalesce(1).write.csv("output/"+Country+"/"+Province+"/"+dataName+"/greatestAvgGF.csv")

    tempDF = growthFactor(df,leastMedDate)
    tempDF.coalesce(1).write.csv("output/"+Country+"/"+Province+"/"+dataName+"/leastMedGF.csv")

    tempDF = growthFactor(df,leastAvgDate)
    tempDF.coalesce(1).write.csv("output/"+Country+"/"+Province+"/"+dataName+"/leastAvgGF.csv")

    tempDF = growthFactor(df,maxGrowthDate)
    tempDF.coalesce(1).write.csv("output/"+Country+"/"+Province+"/"+dataName+"/maxGF.csv")

    tempDF = growthFactor(df,minGrowthDate)
    tempDF.coalesce(1).write.csv("output/"+Country+"/"+Province+"/"+dataName+"/minGF.csv")
  }
  */
}
