package com.revature.CovidAnalysis

import loadpath.LoadPath
import org.apache.spark.sql.Row
import org.apache.spark.storage.StorageLevel._

import scala.Double._
import scala.language.postfixOps
//import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, _}
import org.apache.spark.sql.types.{DecimalType, IntegerType, _}
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}

import java.text.SimpleDateFormat
import java.util.Calendar


object Runner {

  def main(args: Array[String]): Unit = {
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
      .option("delimiter", ",")
      .format("csv")
      .load(LoadPath.hdfs_path + "covid_19_data.csv")
      .toDF()


    //implicit val enc: Encoder[CovidAccum] = Encoders.product[CovidAccum]

    val covid_confirmed_DB = spark.read
      .option("header", true)
      .option("delimiter", ",")
      .format("csv")
      .load(LoadPath.hdfs_path + "time_series_covid_19_confirmed.csv")
      .toDF()

    val covid_confirmed_US_DB = spark.read
      .option("header", true)
      .option("delimiter", ",")
      .format("csv")
      .load(LoadPath.hdfs_path + "time_series_covid_19_confirmed_US.csv")
      .toDF()

    val covid_deaths_DB = spark.read
      .option("header", true)
      .option("delimiter", ",")
      .format("csv")
      .load(LoadPath.hdfs_path + "time_series_covid_19_deaths.csv")
      .toDF()

    val covid_deaths_US_DB = spark.read
      .option("header", true)
      .option("delimiter", ",")
      .format("csv")
      .load(LoadPath.hdfs_path + "time_series_covid_19_deaths_US.csv")
      .toDF()

    val covid_recovered_DB = spark.read
      .option("header", true)
      .option("delimiter", ",")
      .format("csv")
      .load(LoadPath.hdfs_path + "time_series_covid_19_recovered.csv")
      .toDF()

    val filteredDF =
      covid_accum_DB.where($"Confirmed" > 0)


    /**
     * covid_accum_DB.show()
     * covid_confirmed_US_DB.show()
     * covid_confirmed_DB.show()
     * covid_deaths_DB.show()
     * covid_deaths_US_DB.show()
     * covid_recovered_DB.show()
     * */


    val firstConfirmedCountries =
      dataCleanseFilter(
        firstOccurrenceCovid19(covid_accum_DB, "Confirmed", "Country/Region")
          .select("SNo", "ObservationDate", "Country/Region", "Confirmed"),
        "Country/Region"
      )

    val firstConfirmedUSStates =
      dataCleanseFilter(
        firstOccurrenceCovid19(covid_accum_DB, "Confirmed", "Province/State")
          .where($"Country/Region" === "US")
          .select("SNo", "ObservationDate", "Province/State", "Confirmed"),
        "Province/State"
      )

    val accumCountries = attainTargetAccum(covid_accum_DB, "Country/Region")
    val accumUSStates = attainTargetAccum(covid_accum_DB, "Province/State")

/*
    println("Historic Table")
    covid_confirmed_US_DB.show(1)
    println("Increment Table")
    val xyz = incrementGenerator(covid_confirmed_US_DB, true, spark)
    xyz.show(1)
    println("Show the Percent Change Between Days in Historic Data")
    percentChangeGenerator(covid_confirmed_US_DB, true, spark).show(1)
    println("Show the Percent Change Between Days in Incremental Data")
    percentChangeGenerator(xyz, true, spark).show(1)
*/

    val abc = incrementGenerator(covid_confirmed_US_DB, true, spark)
    val mno = localSpikeGenerator(abc, true, spark)
    mno.show(false)
    val efg = incrementGenerator(covid_confirmed_DB, false, spark)
    val qrs = localSpikeGenerator(efg, true, spark)
    qrs.show


    spark.close()
  }

  //do not use with covid_accum_DB
  def changeOverTime(df: DataFrame, startDate: String, numOfDays: Int) = {
    val format = new SimpleDateFormat("M/d/yy")
    val date = format.parse(startDate)
    val c = Calendar.getInstance()
    c.setTime(date)
    c.add(Calendar.DATE, numOfDays)
    val dt = format.format(c.getTime())
    var df_mod = df.select(df("Combined_Key"), df(startDate).cast(IntegerType)).orderBy(desc(startDate))
    val df_temp = df.select(df("Combined_Key").as("_n_"), df(dt).cast(IntegerType)).orderBy(desc(dt))
    df_mod = df_mod.join(df_temp, df_temp("_n_") === df_mod("Combined_Key"), "inner").drop("_n_")
    df_mod.withColumn("delta", df_mod(dt) - df_mod(startDate))
  }

  //only works when date is between 1/23/20-5/2/21
  def growthFactor(df: DataFrame, strDate: String) = {
    val format = new SimpleDateFormat("M/d/yy")
    val date = format.parse(strDate)
    val c = Calendar.getInstance()
    c.setTime(date)
    c.add(Calendar.DATE, -1)
    val dt = format.format(c.getTime())
    var df_mod = df.select(df("Combined_Key").as("_n_"), df(strDate).cast(IntegerType)).orderBy(desc(strDate))
    val df_temp = df.select(df("Combined_Key").as("Location"), df(dt).cast(IntegerType)).orderBy(desc(dt))
    df_mod = df_temp.join(df_mod, df_temp("Location") === df_mod("_n_"), "inner").drop("_n_")
    df_mod.withColumn("delta", (((df_mod(strDate) / df_mod(dt)) - 1) * 100).cast(DecimalType(8, 2))).na.fill(0)
  }


  //Modification on Tim's Change Over Time Method, to output different dates based on input dates and spread
  def twoWeekCatcher(dfx: DataFrame, start: String, spread: Int): String = {
    var startDate = ""
    var pattern = ""
    if (dfx.schema.fieldNames.contains("ObservationDate")) {
      val intermid = dfx.select("ObservationDate").where(col("ObservationDate") === s"${start}").collect
      startDate = intermid(0)(0).toString
      println(s"$startDate")
      pattern = "mm/dd/yyyy"
    }
    else {
      startDate = start
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


  /**
   * First occurence of the occurence type being sought after. Most importantly is including the date of the occurence
   * and is ordered in ascending order.
   *
   * @param covid19AccumDB    = the Covid_19_DB dataframe that contains the loaded data from the csv file
   * @param occurTypeCol      = the occurence type consisting of: Confirmed,Deaths, or Recovered.
   * @param partitionByCol    = the entity that we are trying to gain the occurence from, either Country or States. Could be
   *                          expanded to Continents later.
   * @param partitionByTarget = the target to better filter out results. This target only includes a subset filter based on
   *                          the partitionByCol.
   *                          e.g: target = San Diego County , partitionByCol = State/Province
   * @return the first occurence data frame
   */

  def firstOccurrenceCovid19(covid19AccumDB: DataFrame, occurTypeCol: String, partitionByCol: String, partitionByTarget: String = ""): DataFrame = {

    //removes useless data and adding a formatted date column
    val filteredDF =
      covid19AccumDB.where(s"$occurTypeCol > 0")
        .withColumn("FormattedDate", to_date(covid19AccumDB("ObservationDate"), "MM/dd/yy"))
        .cache()

    //partitioning by given column to order the dates and grab the first instance.
    val window = Window.partitionBy(partitionByCol).orderBy("FormattedDate")
    var firstOccurDF =
      filteredDF.withColumn("rowNum", row_number().over(window))
        .where("rowNum == 1")
        .orderBy("FormattedDate")
        .drop("rowNum", "FormattedDate")
    if (partitionByTarget != "")
      firstOccurDF = firstOccurDF.where(firstOccurDF(partitionByCol).like("%" + partitionByTarget + "%"))
    firstOccurDF

  }

  /**
   * Returns the accumulated values from the Confirmed,Deaths and Recovered according to the targetColumn entity.
   * side note:
   * Using groupBy + agg is not ideal, ... for RDD's, it does not do map side reduce and requires unnecessary shuffling, but because it is a
   * Dataset[Row], it uses the Catalyst Optimizer to optimize it to be the same as the RDD reduceByKey function.
   *
   * @param covid19Accum = only the Accum DF
   * @param targetCol    = is either Country or State
   * @param target       = target is only a subset of targetCol, the country or state/counties to filter the column by, if given
   * @return = Dataframe accumulations
   */
  def attainTargetAccum(covid19Accum: DataFrame, targetCol: String, target: String = ""): DataFrame = {
    val accumDF: DataFrame =
      dataCleanseFilter(
        covid19Accum.select(targetCol, "Confirmed", "Deaths", "Recovered"),
        targetCol,
        target
      )
        .groupBy(targetCol)
        .agg(
          sum("Confirmed").as("Total Confirmed"),
          sum("Deaths").as("Total Deaths"),
          sum("Recovered").as("Total Recovered"))
        .orderBy(targetCol)
        .cache()
    if (target != "")
      return accumDF.where(accumDF(targetCol).like("%" + target))
    accumDF
  }

  /**
   * dataCleanseFilter is a function that filters out all values that are not what are to be targeting in the normal format and in essence
   * makes sure that the dataframe being returned is exactly what the target column is, Countries or States. At this stage
   * of development it only accepts states from the U.S but can be expanded on to be acceptable from states from global countries.
   * The isUSFile is a flag, because of the decision to not fully data clean or format the original dataframe
   * once loaded from CSV, this accounts for the time_series_US csv files that have their column with underscores instead
   * of /.
   *
   * @param toBeCleaned Dataframe to be cleaned while filtered
   * @param targetCol   Column that is being targeted to get the
   * @param target      to be expanded on later => states/provinces from countries other then U.S or U.S State counties.
   * @return
   */
  def dataCleanseFilter(toBeCleaned: DataFrame, targetCol: String, target: String = ""): DataFrame = {
    val statesUS: List[String] = List[String]("Alabama", "Alaska", "American Samoa", "Arizona", "Arkansas", "California",
      "Colorado", "Connecticut", "Delaware", "District of Columbia", "Florida", "Georgia", "Guam", "Hawaii",
      "Idaho", "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts",
      "Michigan", "Minnesota", "Minor Outlying Islands", "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada",
      "New Hampshire", "New Jersey", "New Mexico", "New York", "North Carolina", "North Dakota", "Northern Mariana Islands",
      "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Puerto Rico", "Rhode Island", "South Carolina", "South Dakota", "Tennessee",
      "Texas", "U.S. Virgin Islands", "Utah", "Vermont", "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming")
    val statesAbrevUS: List[String] = List[String]("AK", "AL", "AR", "AS", "AZ", "CA", "CO", "CT", "DC", "DE", "FL",
      "GA", "GU", "HI", "IA", "ID", "IL", "IN", "KS", "KY", "LA", "MA", "MD", "ME", "MI", "MN", "MO", "MP", "MS", "MT",
      "NC", "ND", "NE", "NH", "NJ", "NM", "NV", "NY", "OH", "OK", "OR", "PA", "PR", "RI", "SC", "SD", "TN", "TX", "UM",
      "UT", "VA", "VI", "VT", "WA", "WI", "WV", "WY")
    val countries: List[String] = List[String]("US", "Canada", "Afghanistan", "Albania", "Algeria", "American Samoa",
      "Andorra", "Angola", "Anguilla", "Antarctica", "Antigua and/or Barbuda", "Argentina", "Armenia", "Aruba", "Australia",
      "Austria", "Azerbaijan", "Bahamas", "Bahrain", "Bangladesh", "Barbados", "Belarus", "Belgium", "Belize", "Benin",
      "Bermuda", "Bhutan", "Bolivia", "Bosnia and Herzegovina", "Botswana", "Bouvet Island", "Brazil",
      "British Indian Ocean Territory", "Brunei Darussalam", "Bulgaria", "Burkina Faso", "Burundi", "Cambodia", "Cameroon",
      "Cape Verde", "Cayman Islands", "Central African Republic", "Chad", "Chile", "Mainland China", "Christmas Island", "Cocos (Keeling) " +
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
    var targetList: List[String] = List[String]()

    if (targetCol == "Province/State" || targetCol == "Province_State")
      targetList = statesUS
    else
      targetList = countries


    toBeCleaned.where(toBeCleaned(targetCol).isin(targetList: _*))
  }


  def incrementGenerator(dfx: DataFrame, isUS: Boolean, context: SparkSession): DataFrame = {
    val schemaRetention = dfx.schema
    val trimmerValue = if (isUS) 11 else 4
    val dfxFiller = dfx.na.fill(" ", Seq("*"))
    val squishColumnsTogether = dfxFiller.withColumn("ClumpRow", expr("concat_ws('~',*)"))
    val identityColumns = squishColumnsTogether.columns.filter(b => b.toUpperCase().head >= 'A' && b.toUpperCase.head <= 'Z')
    val refactorColumnSet = squishColumnsTogether.select(identityColumns.head, identityColumns.tail: _*)
    val produceIntArray = udf((a: String) => (a.split("~").drop(trimmerValue).map(x => x.toInt)))
    val dfxWithIntArray = refactorColumnSet.select("*").withColumn("Daily Tallies", produceIntArray(col("ClumpRow"))).drop(col("ClumpRow"))
    val offsetIntArray = udf((x: Seq[Int]) => 0 +: x.dropRight(1))
    val dfxWithOffset = dfxWithIntArray.select("*").withColumn("Daily Offset", offsetIntArray(col("Daily Tallies")))
    val arrayZipper = udf((m: Seq[Int], n: Seq[Int]) => (m zip n).toList)
    val dfxTogether = dfxWithOffset.select("*").withColumn("Yesterday_Today", arrayZipper(col("Daily Offset"), col("Daily Tallies"))).drop(col("Daily Tallies")).drop(col("Daily Offset"))
    val increment = udf((p: Seq[Row]) => p.map(q => (q.getInt(1) - q.getInt(0)).toString()))
    val dfxIncrement = dfxTogether.select("*").withColumn("Increment", increment(col("Yesterday_Today"))).drop(col("Yesterday_Today"))
    val lengthOfArray = dfxIncrement.withColumn("IncrementExpansion", org.apache.spark.sql.functions.size(col("Increment")))
      .selectExpr("max(IncrementExpansion)").head().getInt(0)
    val dfxExpanded = dfxIncrement.select(col("*") +: (0 until lengthOfArray).map(u => dfxIncrement.col("Increment").getItem(u).alias(s"Day $u")): _*).drop("Increment")
    val outputDataFrame = context.createDataFrame(dfxExpanded.rdd, schemaRetention)
    outputDataFrame
  }

  def percentChangeGenerator(dfx: DataFrame, isUS: Boolean, context: SparkSession): DataFrame = {
    val schemaRetention = dfx.schema
    val trimmerValue = if (isUS) 11 else 4
    val dfxFiller = dfx.na.fill(" ", Seq("*"))
    val squishColumnsTogether = dfxFiller.withColumn("ClumpRow", expr("concat_ws('~',*)"))
    val identityColumns = squishColumnsTogether.columns.filter(b => b.toUpperCase().head >= 'A' && b.toUpperCase.head <= 'Z')
    val refactorColumnSet = squishColumnsTogether.select(identityColumns.head, identityColumns.tail: _*)
    val produceIntArray = udf((a: String) => (a.split("~").drop(trimmerValue).map(x => x.toInt)))
    val dfxWithIntArray = refactorColumnSet.select("*").withColumn("Daily Tallies", produceIntArray(col("ClumpRow"))).drop(col("ClumpRow"))
    val offsetIntArray = udf((x: Seq[Int]) => 0 +: x.dropRight(1))
    val dfxWithOffset = dfxWithIntArray.select("*").withColumn("Daily Offset", offsetIntArray(col("Daily Tallies")))
    val arrayZipper = udf((m: Seq[Int], n: Seq[Int]) => (m zip n).toList)
    val dfxTogether = dfxWithOffset.select("*").withColumn("Yesterday_Today", arrayZipper(col("Daily Offset"), col("Daily Tallies"))).drop(col("Daily Tallies")).drop(col("Daily Offset"))
    val increment = udf((p: Seq[Row]) => p.map(q => if (q.getInt(0) != 0) BigDecimal(q.getInt(1).toDouble / q.getInt(0).toDouble - 1).setScale(3, BigDecimal.RoundingMode.HALF_UP).toString else "0.000"))
    val dfxIncrement = dfxTogether.select("*").withColumn("Increment", increment(col("Yesterday_Today"))).drop(col("Yesterday_Today"))
    val lengthOfArray = dfxIncrement.withColumn("IncrementExpansion", org.apache.spark.sql.functions.size(col("Increment")))
      .selectExpr("max(IncrementExpansion)").head().getInt(0)
    val dfxExpanded = dfxIncrement.select(col("*") +: (0 until lengthOfArray).map(u => dfxIncrement.col("Increment").getItem(u).alias(s"Day $u")): _*).drop("Increment")
    val outputDataFrame = context.createDataFrame(dfxExpanded.rdd, schemaRetention)
    outputDataFrame
  }

  //Desires a DataFrame as an output from the incrementGenerator method, can technically take Historic Data
  def localSpikeGenerator(dfx:DataFrame, isUS:Boolean, context:SparkSession):DataFrame={
    val schemaRetention = dfx.schema
    val trimmerValue = if (isUS) 11 else 4
    val dfxFiller = dfx.na.fill(" ", Seq("*"))
    val squishColumnsTogether = dfxFiller.withColumn("ClumpRow", expr("concat_ws('~',*)"))
    val identityColumns = squishColumnsTogether.columns.filter(b => b.toUpperCase().head >= 'A' && b.toUpperCase.head <= 'Z')
    val refactorColumnSet = squishColumnsTogether.select(identityColumns.head, identityColumns.tail: _*)
    val produceIntArray = udf((a: String) => a.split("~").drop(trimmerValue).map(x => x.toInt))
    val dfxWithIntArray = refactorColumnSet.select("*")
      .withColumn("Increments Ordered by Date", produceIntArray(col("ClumpRow")))
      .drop(col("ClumpRow"))
    val ratioComparedToMax = udf((asd:Seq[Int]) => asd.map(jkl => if (asd.max != 0) (jkl/asd.max).toDouble else 0.0))
    val dfxWithMax = dfxWithIntArray.select("*")
      .withColumn("Ratio to Max", ratioComparedToMax(col("Increments Ordered by Date")))
    val peakFilter = udf((xyz:Seq[Double]) => xyz.filter(a => if (xyz.max != 0) a >= 0.95*xyz.max else false) )
    val peakCounter = udf((abc:Seq[Double])=> abc.size)
    val peakValue = udf((efg:Seq[Double]) => efg.max)
    val dfxHighDays = dfxWithMax.select("*")
      .withColumn("List of High Days", peakFilter(col("Ratio to Max")))
      .withColumn("Count of High Days", peakCounter(col("List of High Days")))
      .withColumn("Highest Daily Value", peakValue(col("Increments Ordered by Date")))
    if (isUS) dfxHighDays.select("Combined Key", "Count of High Days", "Highest Daily Value")
      .orderBy(desc("Count of High Days"), desc("Highest Daily Value"))
    else dfxHighDays.select("Combined Key","Count of High Days", "Highest Daily Value")
      .orderBy(desc("Count of High Days"), desc("Highest Daily Value"))
  }

}



