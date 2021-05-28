package com.revature.CovidAnalysis

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoders, Row, SparkSession}
import loadpath.LoadPath
import org.apache.spark.sql.catalyst.expressions.Or
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.functions.col

import java.beans.Encoder
//import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.sources._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.sources.And
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

import org.apache.spark.sql.{DataFrame, SparkSession}
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
    //val california = covid_accum_DB.where($"Country/Region" === "US" && $"Province/State" === "California")
    //  .select($"Deaths".cast(IntegerType), $"ObservationDate").orderBy(desc("Deaths"))
    //california.show()


    /**
    covid_accum_DB.show()
    covid_confirmed_US_DB.show()
    covid_confirmed_DB.show()
    covid_deaths_DB.show()
    covid_deaths_US_DB.show()
    covid_recovered_DB.show()
    **/


    val dt = firstOccurrence_US(covid_accum_DB,"Deaths")
    val dy = firstOccurrence_Countries(covid_accum_DB,"Deaths");

    dt.show(60)
    dy.show(300)
    /**
    val california = dt.where($"Country/Region" === "US")
      .where($"Province/State".like("%California%")
        || $"Province/State".like("%CA%"))
    california.show(58)
    **/
    //spikeAtTarget(covid_accum_DB, "Country/Region", "Brazil", "01/01/2021", 7, 5.0) // Determine whether there is a spike created from some day
    //latestValuesForAccumulatedTable(covid_accum_DB, "Country/Region", "05/02/2021") //latest 'deaths, confirms, and recoveries' based on input day on Big Set
    //latestValueForSubTables(covid_deaths_DB, "Country/Region", "5/2/21") //latest 'deaths/confirms/recoveries' based on input day on Sub Sets
    //latestValueForSubTables(covid_confirmed_DB, "Province/State", "5/2/21")
      //we should JUST go by country, because all the undocumented provinces/states across countries will get lumped together

    //covid_accum_DB.select("*").where(col("ObservationDate") === "05/02/2021" && col("Country/Region").like("U%S%")).show


    //val joina = trueTotalDeaths.withColumnRenamed("sum(Deaths)","Deaths").join(trueTotalRecovered,Seq("Country/Region"), "Left").withColumnRenamed("sum(Recovered)", "Recovered") //Unite Deaths and Recoveries
    //val joinb = joina.join(trueTotalConfirmed, Seq("Country/Region"), "Left").withColumnRenamed("sum(Confirmed)","Confirmed")   //Unite Deaths, Recoveries and Confirmations

    //val ratio = joinb.withColumn("Decimal Deaths", joinb.col("Deaths").cast(LongType)/joinb.col("Confirmed").cast(LongType)) .withColumn("Decimal Recoveries", joinb.col("Recovered").cast(LongType)/joinb.col("Confirmed").cast(LongType))

    //joinb.show()
    //val FiveMostAffected = ratio.orderBy(desc("Confirmed")).show(5)
    //val FiveLeastAffected = ratio.orderBy(asc("Confirmed")).show(5)

    //val FiveMostDeaths = ratio.orderBy(desc("Decimal Deaths")).show(5)
    //val FiveLeastDeaths = ratio.orderBy(asc("Decimal Deaths")).show(5) ).show

    //val FiveMostRecoveries = ratio.orderBy(desc("Decimal Recoveries")).show(5)
    //val FiveLeastRecoveries = ratio.orderBy(asc("Decimal Recoveries")).show(5)


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

  def spikeAtTarget(dfSource:DataFrame, targetFilter:String, target:String, initialDate:String, dateRange:Int, spikeFactor:Double):Unit={ //what to Return?
    // dfFiller is the dataframe we want to look into
    // targetFilter is the column we want to target
    // target is the value of the column set we want to look through
    // startDate is an arbitrary date we want to look at, for example June 12th is Brazilian Valentine's Day
    // dateRange is the number of days after the startDate. Incubation is typically at most 14 days
    //      then, set dateRange to 10-14 to see how many people are actually getting infected by it
    //      alternatively, set the date to the 10th and look at 18 days for date Range to get a wider growth scope
    // spikeFactor is the facter by which some confirmed-case growth is increased
    //      (i.e. spikeFactor = 3.0, in 14 days we go from 10 -> 25 means the growth factor is 2.5 < 3.0 and so it does not 'count' as a spike)

    var spikeShell = dfSource.where(col(s"${targetFilter}") === s"${target}") //create DF from input DF as data filtered by target
    //spikeShell.show()
    val startDate = twoWeekCatcher(dfSource, initialDate, 14)
    val endDate = twoWeekCatcher(dfSource, initialDate, dateRange + 14) // get date after a number of days equal to dateRange

    println(s"$startDate")
    println(s"$endDate")

    var startColName = ""
    var endColName = ""
    var spikeStart = spikeShell
    var spikeEnd = spikeShell

    if (dfSource.schema.fieldNames.contains("ObservationDate")){
      println(" 'this' ")
      startColName = "ObservationDate"
      endColName = "ObservationDate"
      spikeStart = spikeShell.select(col("Confirmed").as("Start Confirmed"),col("Country/Region").as("CR"),col("Province/State").as("PS")).where(col(s"${startColName}") === s"${startDate}" && col(s"$targetFilter") === s"$target")// get confirmed cases on start date
      spikeEnd = spikeShell.select(col("Confirmed").as("End Confirmed"),col("Country/Region").as("CR"),col("Province/State").as("PS")).where(col(s"${endColName}") === s"${endDate}" && col(s"$targetFilter") === s"$target")// get confirmed cases on end date
    }

    else{
      println(" 'that' ")
      startColName = startDate
      endColName = endDate
      spikeStart = spikeShell.select(col(s"${startColName}").as("Start Confirmed"),col("Country/Region").as("CR"),col("Province/State").as("PS")).where(col(s"$targetFilter") === s"$target")// get confirmed cases on start date
      spikeEnd = spikeShell.select(col(s"${endColName}").as("End Confirmed"),col("Country/Region").as("CR"),col("Province/State").as("PS")).where(col(s"$targetFilter") === s"$target")// get confirmed cases on end date
    }

    println(" 'Existential Terror Noises' ")
    spikeStart.show(false)
    println("I can't take it anymore!")
    spikeEnd.show(false)

    val spikeTime = spikeStart.join(spikeEnd, Seq("CR","PS"),"left").withColumn("SpikeFactor", ((spikeEnd.col("End Confirmed").cast(LongType)/spikeStart.col("Start Confirmed").cast(LongType)).minus(1)).multiply(100))
    println("WI just wanna DIE~!")
    spikeTime.select("*").show(false)
    val check = spikeTime.select("*").where(col("SpikeFactor") >= spikeFactor)
    println("we ALL wanna die!")
    check.show(false)
  }


  //Modification on Tim's Change Over Time Method, to output different dates based on input dates and spread
  def twoWeekCatcher(dfx:DataFrame, start:String, spread:Int):String={
    var startDate=""
    var pattern = ""
    if(dfx.schema.fieldNames.contains("ObservationDate")){
      val intermid = dfx.select("ObservationDate").where(col("ObservationDate") === s"${start}").collect
      startDate = intermid(0)(0).toString
      println(s"$startDate")
      pattern = "mm/dd/yyyy"
    }
    else {
      startDate=start
      pattern = "M/d/yy"
    }
      val format = new SimpleDateFormat(pattern)
      val date = format.parse(startDate)
      val c = Calendar.getInstance()
      c.setTime(date)
      c.add(Calendar.DATE, spread)
      val dt = format.format(c.getTime())
      dt
  }

  def latestValuesForAccumulatedTable(dfx:DataFrame, targetColumn:String, targetDate:String):Unit={
    val dateTarget = twoWeekCatcher(dfx, targetDate, 0)
    val dfxOne = dfx.select("*").where(col("ObservationDate") === dateTarget)
    val dfxPrint= dfxOne.select("*")
        .groupBy(targetColumn)
        .agg(sum("Confirmed").as("Total Confirmations"), sum("Deaths").as("Total Deaths"), sum("Recovered").as("Total Recoveries"))
        .orderBy(desc("Total Deaths"))
        .show(50,false)
  }

  def latestValueForSubTables(dfx:DataFrame, targetColumn:String, targetDate:String):Unit={
    val dateTarget = twoWeekCatcher(dfx, targetDate, 0)
    val dfxOne = dfx.select(col(targetColumn).as("key"), col(dateTarget).as("value"))
      .groupBy("key")
      .agg(sum("value").as("value"))
      .orderBy(desc("value"))
      .as[StepShell](Encoders.product[StepShell])
      .show(50,false)
  }

  //find the difference of 'deaths/recoveries/confirms' between days
  def stepFinder(dfx:DataFrame, targetCountry:String):Unit={
    val localRDD = dfx.select("Country/Region", "Province/State")
      .withColumn("Caught Increment", dfx.col("Confirmed").cast(LongType))
      .withColumn("Dead Increment", dfx.col("Confirmed").cast(LongType))
      .withColumn("Revived Increment", dfx.col("Confirmed").cast(LongType)).rdd
  }


  /**
   * @param df, occurTypeCol = "Deaths", occurByCol = "Country/Region"
   * */

  def firstOccurrence(dfSource: DataFrame, occurTypeCol: String, occurByCol: String, occurByTarget: String = ""): DataFrame = {

    //removes useless data and adding a formatted date column
    val filteredDF =
      dfSource.where(s"$occurTypeCol > 0")
      .withColumn("FormattedDate",to_date(dfSource("ObservationDate"), "MM/dd/yy"))

    //partitioning by given column to order the dates and grab the first instance.
    val window = Window.partitionBy(occurByCol).orderBy("FormattedDate")
    var firstOccurDF =
          filteredDF.withColumn("rowNum", row_number().over(window))
          .where("rowNum == 1" )
          .drop("rowNum")
    if(occurByTarget != "")
      firstOccurDF = firstOccurDF.where(firstOccurDF(occurByCol).like("%"+occurByTarget))
    firstOccurDF

  }

  def firstOccurrence_US (dfSource: DataFrame, occurType: String): DataFrame = {
    val firstOccurUS = firstOccurrence(dfSource,occurType, "Province/State").where(col("Country/Region") === "US")
    val states: List[String] = List[String]("Alabama", "Alaska", "American Samoa", "Arizona", "Arkansas", "California",
      "Colorado", "Connecticut", "Delaware", "District of Columbia", "Florida", "Georgia", "Guam", "Hawaii",
      "Idaho", "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts",
      "Michigan", "Minnesota", "Minor Outlying Islands", "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada",
      "New Hampshire", "New Jersey", "New Mexico", "New York", "North Carolina", "North Dakota", "Northern Mariana Islands",
      "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Puerto Rico", "Rhode Island", "South Carolina", "South Dakota", "Tennessee",
      "Texas", "U.S. Virgin Islands", "Utah", "Vermont", "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming")
    val statesAbrev: List[String] = List[String]("AK", "AL", "AR", "AS", "AZ", "CA", "CO", "CT", "DC", "DE", "FL",
      "GA", "GU", "HI", "IA", "ID", "IL", "IN", "KS", "KY", "LA", "MA", "MD", "ME", "MI", "MN", "MO", "MP", "MS", "MT",
      "NC", "ND", "NE", "NH", "NJ", "NM", "NV", "NY", "OH", "OK", "OR", "PA", "PR", "RI", "SC", "SD", "TN", "TX", "UM",
      "UT", "VA", "VI", "VT", "WA", "WI", "WV", "WY")

    val cleansed =
      firstOccurUS
        .where(firstOccurUS("Province/State")
        .isin(states:_*))
        .orderBy("FormattedDate")
        .drop("FormattedDate")

    cleansed
  }

  def firstOccurrence_Countries (dfSource: DataFrame, occurType: String): DataFrame = {
    val firstOccurGlobal = firstOccurrence(dfSource,occurType, "Country/Region")
    val countries: List[String] = List[String]("US", "Canada", "Afghanistan", "Albania", "Algeria", "American Samoa",
      "Andorra", "Angola", "Anguilla", "Antarctica", "Antigua and/or Barbuda", "Argentina", "Armenia", "Aruba", "Australia",
      "Austria", "Azerbaijan", "Bahamas", "Bahrain", "Bangladesh", "Barbados", "Belarus", "Belgium", "Belize", "Benin",
      "Bermuda", "Bhutan", "Bolivia", "Bosnia and Herzegovina", "Botswana", "Bouvet Island", "Brazil",
      "British Indian Ocean Territory", "Brunei Darussalam", "Bulgaria", "Burkina Faso", "Burundi", "Cambodia", "Cameroon",
      "Cape Verde", "Cayman Islands", "Central African Republic", "Chad", "Chile", "China", "Christmas Island", "Cocos (Keeling) " +
        "Islands", "Colombia", "Comoros", "Congo", "Cook Islands", "Costa Rica", "Croatia (Hrvatska)", "Cuba", "Cyprus",
      "Czech Republic", "Denmark", "Djibouti", "Dominica", "Dominican Republic", "East Timor", "Ecuador", "Egypt",
      "El Salvador", "Equatorial Guinea", "Eritrea", "Estonia", "Ethiopia", "Falkland Islands (Malvinas)", "Faroe Islands",
      "Fiji", "Finland", "France", "France, Metropolitan", "French Guiana", "French Polynesia", "French Southern Territories",
      "Gabon", "Gambia", "Georgia", "Germany", "Ghana", "Gibraltar", "Greece", "Greenland", "Grenada", "Guadeloupe", "Guam",
      "Guatemala", "Guinea", "Guinea-Bissau", "Guyana", "Haiti", "Heard and McDonald Islands", "Honduras", "Hong Kong",
      "Hungary", "Iceland", "India", "Indonesia", "Iran (Islamic Republic of)", "Iraq", "Ireland", "Israel", "Italy",
      "Ivory Coast", "Jamaica", "Japan", "Jordan", "Kazakhstan", "Kenya", "Kiribati", "Korea, Democratic People's Republic of",
      "Korea, Republic of", "Kosovo", "Kuwait", "Kyrgyzstan", "Lao People's Democratic Republic", "Latvia", "Lebanon", "Lesotho",
      "Liberia", "Libyan Arab Jamahiriya", "Liechtenstein", "Lithuania", "Luxembourg", "Macau", "Macedonia", "Madagascar",
      "Malawi", "Malaysia", "Maldives", "Mali", "Malta", "Marshall Islands", "Martinique", "Mauritania", "Mauritius",
      "Mayotte", "Mexico", "Micronesia, Federated States of", "Moldova, Republic of", "Monaco", "Mongolia", "Montserrat",
      "Morocco", "Mozambique", "Myanmar", "Namibia", "Nauru", "Nepal", "Netherlands", "Netherlands Antilles", "New Caledonia",
      "New Zealand", "Nicaragua", "Niger", "Nigeria", "Niue", "Norfolk Island", "Northern Mariana Islands", "Norway", "Oman",
      "Pakistan", "Palau", "Panama", "Papua New Guinea", "Paraguay", "Peru", "Philippines", "Pitcairn", "Poland", "Portugal",
      "Puerto Rico", "Qatar", "Reunion", "Romania", "Russian Federation", "Rwanda", "Saint Kitts and Nevis", "Saint Lucia",
      "Saint Vincent and the Grenadines", "Samoa", "San Marino", "Sao Tome and Principe", "Saudi Arabia", "Senegal",
      "Seychelles", "Sierra Leone", "Singapore", "Slovakia", "Slovenia", "Solomon Islands", "Somalia", "South Africa",
      "South Georgia South Sandwich Islands", "South Sudan", "Spain", "Sri Lanka", "St. Helena", "St. Pierre and Miquelon",
      "Sudan", "Suriname", "Svalbard and Jan Mayen Islands", "Swaziland", "Sweden", "Switzerland", "Syrian Arab Republic",
      "Taiwan", "Tajikistan", "Tanzania, United Republic of", "Thailand", "Togo", "Tokelau", "Tonga", "Trinidad and Tobago",
      "Tunisia", "Turkey", "Turkmenistan", "Turks and Caicos Islands", "Tuvalu", "Uganda", "Ukraine", "United Arab Emirates",
      "United Kingdom", "United States minor outlying islands", "Uruguay", "Uzbekistan", "Vanuatu", "Vatican City State",
      "Venezuela", "Vietnam", "Virgin Islands (British)", "Virgin Islands (U.S.)", "Wallis and Futuna Islands", "Western Sahara",
      "Yemen", "Yugoslavia", "Zaire", "Zambia", "Zimbabwe")

    val cleansed =
      firstOccurGlobal
        .where(firstOccurGlobal("Country/Region")
          .isin(countries:_*))
        .orderBy("FormattedDate")
        .drop("FormattedDate")

    cleansed
  }



}
