package com.revature.CovidAnalysis

import loadpath.LoadPath
import org.apache.spark.sql.Row
import org.apache.spark.storage.StorageLevel._
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
    //val california = covid_accum_DB.where($"Country/Region" === "US" && $"Province/State" === "California")
    //  .select($"Deaths".cast(IntegerType), $"ObservationDate").orderBy(desc("Deaths"))
    //california.show()


    //val stateDF = covid_confirmed_US_DB.select(covid_confirmed_US_DB("Province_State")).distinct()
    //val stateList = stateDF.collect.map(_ (0)).toList
    //println(stateList)
    //stateList.foreach(f=>allGrowthFactorDataPoints(covid_confirmed_US_DB.filter('Province_State===f.toString).na.fill("").filter('Admin2=!="Unassigned"),"USA", f.toString,"Confirmed Cases"))
    /**
     * covid_accum_DB.show()
     * covid_confirmed_US_DB.show()
     * covid_confirmed_DB.show()
     * covid_deaths_DB.show()
     * covid_deaths_US_DB.show()
     * covid_recovered_DB.show()
     * */




    val firstConfirmedCountries=
      dataCleanseFilter(
        firstOccurrenceCovid19(covid_accum_DB,"Confirmed","Country/Region")
          .select("SNo","ObservationDate","Country/Region","Confirmed"),
        "Country/Region"
    )

    val firstConfirmedUSStates =
      dataCleanseFilter(
        firstOccurrenceCovid19(covid_accum_DB,"Confirmed","Province/State")
          .where($"Country/Region" === "US")
          .select("SNo","ObservationDate","Province/State","Confirmed"),
        "Province/State"
      )

    val accumCountries = attainTargetAccum(covid_accum_DB,"Country/Region")
    val accumUSStates = attainTargetAccum(covid_accum_DB, "Province/State")

    //firstConfirmedUSStates.show()
    //firstConfirmedCountries.show()
    //accumCountries.show()
    //accumUSStates.show()



    // Create a Set of DataFrames that are transformations of the Five original Subsets from Historic Values to Incremental Values
    //val spikeProvider_US_C = incrementGenerator(covid_confirmed_US_DB, true, spark).cache()
    //val spikeProvider_US_D = incrementGenerator(covid_deaths_US_DB, true, spark).persist(MEMORY_ONLY)
    //val spikeProvider_W_C = incrementGenerator(covid_confirmed_DB, false, spark).persist(MEMORY_AND_DISK)
    //val spikeProvider_W_D = incrementGenerator(covid_deaths_DB, false, spark).persist(DISK_ONLY)
    //val spikeProvider_W_R = incrementGenerator(covid_recovered_DB, false, spark).persist(MEMORY_ONLY_SER)


    println("Historic Table")
    covid_confirmed_US_DB.show(1)

    println("Increment Table")
    val xyz = incrementGenerator(covid_confirmed_US_DB, true, spark)
    xyz.show(1)

    println("Show the Percent Change Between Days in Historic Data")
    percentChangeGenerator(covid_confirmed_US_DB, true, spark).show(1)

    println("Show the Percent Change Between Days in Incremental Data")
    percentChangeGenerator(xyz,true, spark).show(1)




    /*
    println("Was there a spike in Confirmed Cases in Texas due to Christmas 2020?")
    spikeAtTarget(spikeProvider_US_C, "Province_State", "Texas", "12/25/2020", 10, 15, true)

    println("Was there a spike in Confirmed Deaths in New York due to Independence Day 2020?")
    spikeAtTarget(spikeProvider_US_D, "Province_State", "New York", "07/04/2020", 10, 15,true)

    println("Was there a spike in Confirmed Cases in Iran at the end of Ramadan 2020?")
    spikeAtTarget(spikeProvider_W_C, "Country/Region", "Iran", "05/12/2020", 10, 15,false)

    println("Was there a spike in Confirmed Deaths in Mainland China due to Lunar New Year 2021?")
    spikeAtTarget(spikeProvider_W_D, "Country/Region", "China", "02/12/2021", 10, 15, false)

    println("Was there a spike in Confirmed Recoveries in Russia due to Russia Day 2020?")
    spikeAtTarget(spikeProvider_W_R, "Country/Region", "Russia", "06/12/2020", 10, 15, false)
    */

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

  def spikeAtTarget(dfFiller: DataFrame, targetFilter: String, target: String, initialDate: String, dateRange: Int, spikeFactor: Double, isUS:Boolean): Unit = { //what to Return?
    // dfFiller is the dataframe we want to look into
    // targetFilter is the column we want to target
    // target is the value of the column set we want to look through
    // startDate is an arbitrary date we want to look at, for example June 12th is Brazilian Valentine's Day
    // dateRange is the number of days after the startDate. Incubation is typically at most 14 days
    //      then, set dateRange to 10-14 to see how many people are actually getting infected by it
    //      alternatively, set the date to the 10th and look at 18 days for date Range to get a wider growth scope
    // spikeFactor is the facter by which some confirmed-case growth is increased
    //      (i.e. spikeFactor = 3.0, in 14 days we go from 10 -> 25 means the growth factor is 2.5 < 3.0 and so it does not 'count' as a spike)

    var spikeShell = dfFiller.select("*").where(col(s"${targetFilter}") === s"${target}") //create DF from input DF as data filtered by target
    val startDate = twoWeekCatcher(spikeShell, initialDate, 14)
    val endDate = twoWeekCatcher(spikeShell, initialDate, dateRange + 14) // get date after a number of days equal to dateRange

    println(s"$startDate")
    println(s"$endDate")

    var startColName = ""
    var endColName = ""
    var spikeStart = spikeShell
    var spikeEnd = spikeShell

    val cr = if (isUS) "Country_Region" else "Country/Region"
    val ps = if (isUS) "Province_State" else "Province/State"

    if (dfFiller.schema.fieldNames.contains("ObservationDate")) {
      startColName = "ObservationDate"
      endColName = "ObservationDate"
      spikeStart = spikeShell.select(col("Confirmed").as("Start Confirmed").cast(IntegerType), col(cr).as("CR"), col(ps).as("PS")).where(col(s"${startColName}") === s"${startDate}" && col(s"$targetFilter") === s"$target") // get confirmed cases on start date
      spikeEnd = spikeShell.select(col("Confirmed").as("End Confirmed").cast(IntegerType), col(cr).as("CR"), col(ps).as("PS")).where(col(s"${endColName}") === s"${endDate}" && col(s"$targetFilter") === s"$target") // get confirmed cases on end date
    }

    else {
      startColName = startDate
      endColName = endDate
      spikeStart = spikeShell.select(col(s"${startColName}").as("Start Confirmed").cast(IntegerType), col(cr).as("CR"), col(ps).as("PS")).where(col(s"$targetFilter") === s"$target")
      spikeEnd = spikeShell.select(col(s"${endColName}").as("End Confirmed").cast(IntegerType), col(cr).as("CR"), col(ps).as("PS")).where(col(s"$targetFilter") === s"$target")
    }

    val sumSS = spikeStart.groupBy("CR").sum("Start Confirmed").withColumnRenamed("sum(Start Confirmed)","Start Total")
    val sumSE = spikeEnd.groupBy("CR").sum("End Confirmed").withColumnRenamed("sum(End Confirmed)", "End Total")

    sumSS.show()
    sumSE.show()

    val spikeTime = sumSS.join(sumSE, sumSS("CR") <=> sumSE("CR"), "left")
      .withColumn("Percent Change", ((sumSE.col("End Total").cast(LongType) / sumSS.col("Start Total").cast(LongType)).minus(1)).multiply(100))
    spikeTime.select("*").show(false)
    //val check = spikeTime.select("Percent Change").first()
    //if(check > )
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

  def latestValuesForAccumulatedTable(dfx: DataFrame, targetColumn: String, targetDate: String): Unit = {
    val dateTarget = twoWeekCatcher(dfx, targetDate, 0)
    val dfxOne = dfx.select("*").where(col("ObservationDate") === dateTarget)
    val dfxPrint = dfxOne.select("*")
      .groupBy(targetColumn)
      .agg(sum("Confirmed").as("Total Confirmations"), sum("Deaths").as("Total Deaths"), sum("Recovered").as("Total Recoveries"))
      .orderBy(desc("Total Deaths"))
      .show(50, false)
  }

  def latestValueForSubTables(dfx: DataFrame, targetColumn: String, targetDate: String): Unit = {
    val dateTarget = twoWeekCatcher(dfx, targetDate, 0)
    val dfxOne = dfx.select(col(targetColumn).as("key"), col(dateTarget).as("value"))
      .groupBy("key")
      .agg(sum("value").as("value"))
      .orderBy(desc("value"))
      .as[StepShell](Encoders.product[StepShell])
      .show(50, false)
  }

  //find the difference of 'deaths/recoveries/confirms' between days
  def stepFinder(dfx: DataFrame, targetCountry: String): Unit = {
    val localRDD = dfx.select("Country/Region", "Province/State")
      .withColumn("Caught Increment", dfx.col("Confirmed").cast(LongType))
      .withColumn("Dead Increment", dfx.col("Deaths").cast(LongType))
      .withColumn("Revived Increment", dfx.col("Recovered").cast(LongType)).rdd
    //localRDD.groupBy(w => w.)
  }


  /**
   * First occurence of the occurence type being sought after. Most importantly is including the date of the occurence
   * and is ordered in ascending order.
   * @param covid19AccumDB = the Covid_19_DB dataframe that contains the loaded data from the csv file
   * @param occurTypeCol = the occurence type consisting of: Confirmed,Deaths, or Recovered.
   * @param partitionByCol = the entity that we are trying to gain the occurence from, either Country or States. Could be
   *                       expanded to Continents later.
   * @param partitionByTarget = the target to better filter out results. This target only includes a subset filter based on
   *                          the partitionByCol.
   *                          e.g: target = San Diego County , partitionByCol = State/Province
   * @return the first occurence data frame
   */

  def firstOccurrenceCovid19(covid19AccumDB: DataFrame, occurTypeCol: String, partitionByCol: String, partitionByTarget: String = ""): DataFrame = {

    //removes useless data and adding a formatted date column
    val filteredDF =
      covid19AccumDB.where(s"$occurTypeCol > 0")
        .withColumn("FormattedDate",to_date(covid19AccumDB("ObservationDate"), "MM/dd/yy"))
        .cache()

    //partitioning by given column to order the dates and grab the first instance.
    val window = Window.partitionBy(partitionByCol).orderBy("FormattedDate")
    var firstOccurDF =
      filteredDF.withColumn("rowNum", row_number().over(window))
        .where("rowNum == 1" )
        .orderBy("FormattedDate")
        .drop("rowNum", "FormattedDate")
    if(partitionByTarget != "")
      firstOccurDF = firstOccurDF.where(firstOccurDF(partitionByCol).like("%"+partitionByTarget+"%"))
    firstOccurDF

  }

  /**
   *  Returns the accumulated values from the Confirmed,Deaths and Recovered according to the targetColumn entity.
   * side note:
   * Using groupBy + agg is not ideal, ... for RDD's, it does not do map side reduce and requires unnecessary shuffling, but because it is a
   * Dataset[Row], it uses the Catalyst Optimizer to optimize it to be the same as the RDD reduceByKey function.
   *
   * @param covid19Accum = only the Accum DF
   * @param targetCol = is either Country or State
   * @param target = target is only a subset of targetCol, the country or state/counties to filter the column by, if given
   * @return = Dataframe accumulations
   */
  def attainTargetAccum(covid19Accum: DataFrame, targetCol: String, target: String = ""): DataFrame = {
    val accumDF: DataFrame =
      dataCleanseFilter(
        covid19Accum.select(targetCol,"Confirmed","Deaths","Recovered"),
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
    if(target != "")
      return accumDF.where(accumDF(targetCol).like("%"+target))
    accumDF
  }

  /**
   *  dataCleanseFilter is a function that filters out all values that are not what are to be targeting in the normal format and in essence
   *  makes sure that the dataframe being returned is exactly what the target column is, Countries or States. At this stage
   *  of development it only accepts states from the U.S but can be expanded on to be acceptable from states from global countries.
   *  The isUSFile is a flag, because of the decision to not fully data clean or format the original dataframe
   *  once loaded from CSV, this accounts for the time_series_US csv files that have their column with underscores instead
   *  of /.
   * @param toBeCleaned Dataframe to be cleaned while filtered
   * @param targetCol Column that is being targeted to get the
   * @param target to be expanded on later => states/provinces from countries other then U.S or U.S State counties.
   * @return
   */
  def dataCleanseFilter(toBeCleaned:DataFrame, targetCol: String, target: String = ""): DataFrame = {
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

    if(targetCol == "Province/State" || targetCol == "Province_State")
      targetList = statesUS
    else
      targetList = countries


    toBeCleaned.where(toBeCleaned(targetCol).isin(targetList:_*))
  }


  def incrementGenerator(dfx: DataFrame, isUS:Boolean, context:SparkSession): DataFrame = {
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

  def percentChangeGenerator(dfx: DataFrame, isUS:Boolean, context:SparkSession): DataFrame = {
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
    val increment = udf((p: Seq[Row]) => p.map(q => if (q.getInt(0) != 0) BigDecimal(q.getInt(1).toDouble/q.getInt(0).toDouble-1).setScale(2, BigDecimal.RoundingMode.HALF_UP).toString else BigDecimal(q.getInt(1).toDouble/1.0).setScale(2,BigDecimal.RoundingMode.HALF_UP).toString()))
    val dfxIncrement = dfxTogether.select("*").withColumn("Increment", increment(col("Yesterday_Today"))).drop(col("Yesterday_Today"))
    val lengthOfArray = dfxIncrement.withColumn("IncrementExpansion", org.apache.spark.sql.functions.size(col("Increment")))
      .selectExpr("max(IncrementExpansion)").head().getInt(0)
    val dfxExpanded = dfxIncrement.select(col("*") +: (0 until lengthOfArray).map(u => dfxIncrement.col("Increment").getItem(u).alias(s"Day $u")): _*).drop("Increment")
    val outputDataFrame = context.createDataFrame(dfxExpanded.rdd, schemaRetention)
    outputDataFrame
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

  /**
    covid_confirmed_US_DB.show()
    covid_confirmed_DB.show()
    covid_deaths_DB.show()
    covid_deaths_US_DB.show()
    covid_recovered_DB.show()
   **/
  // take one of the above five dataframes and create a dataframe where each row has a field containing an array
  // whose elements are indicative of the day-toi-day increase in values, rather than a historic total for each day
  // Note that if you are looking at the US in particular, pass true in the second parameter. Otherwise pass false.

  def incrementGenerator(dfx:DataFrame, isUS:Boolean):Unit={
    var trimmerValue = 0
    if(isUS)  {trimmerValue = 13}
    else {trimmerValue = 4}

    val dfxFiller = dfx.na.fill(" ", Seq("*"))
    val squishColumnsTogether = dfxFiller.withColumn("ClumpRow", expr("concat_ws(',',*)"))
    val renameAllColumns = squishColumnsTogether.select(col("Country_Region").as("CR"), col("Province_State").as("PS"), col("Lat").as("LAT"), col("Long_").as("LONG"), col("ClumpRow").as("All"))
    val produceIntArray = udf((a:String) => (a.split(",").drop(trimmerValue).map(x => x.toInt)))
    val dfxWithIntArray = renameAllColumns.select("*").withColumn("Daily Tallies", produceIntArray(col("All"))).drop(col("All"))
    dfxWithIntArray.printSchema()
    dfxWithIntArray.show
    val offsetIntArray = udf((x:Array[Int]) => 0+:x.dropRight(1))
    val dfxWithOffset = dfxWithIntArray.select("*").withColumn("Daily Offset", offsetIntArray(col("Daily Tallies")))
    val arrayZipper = udf((m:Array[Int], n:Array[Int]) => (m zip n).toList)
    val dfxTogether = dfxWithOffset.select("*").withColumn("Yesterday_Today", arrayZipper(col("Daily Tallies"),col("Daily Offset"))).drop(col("Daily Tallies")).drop(col("Daily Offset"))
    val increment = udf( (p:Array[(Int, Int)]) => p.map(q => q._1-q._2))
    val dfxIncrement = dfxTogether.select(col("CR"),col("PS"), col("Yesterday_Today")).withColumn("Increment", increment(col("Yesterday_Today"))).drop(col("Yesterday_Today"))
    dfxIncrement.show()
    dfxIncrement.collect()
    dfxIncrement.collectAsList()

    /*
    val dfxFiller = dfx.na.fill(" ", Seq("*"))
    val squishColumnsTogether = dfxFiller.withColumn("ClumpRow", expr("concat_ws(',',*)"))
    val renameAllColumns = squishColumnsTogether.select(col("Country_Region").as("CR"), col("Province_State").as("PS"), col("Lat").as("LAT"), col("Long_").as("LONG"), col("ClumpRow").as("All"))
    // Below is a REPL UDF that does the following:
    //		splits each element such that the string is now an Array of Strings
    //		drop the first four elements, since they are not numbers
    //		convert the numbers into integers such that the output is an Array of Integers
    val produceIntArray = udf((a:String) => (a.split(",").drop(trimmerValue).map(x => x.toInt)))
    val dfxWithIntArray = renameAllColumns.select("*").withColumn("Daily Tallies", produceIntArray(col("All"))).drop(col("All"))
    val offsetIntArray = udf((x:Array[Int]) => (0+:x.dropRight(1)))
    val dfxWithOffset = dfxWithIntArray.select("*").withColumn("Daily Offset", offsetIntArray(col("Daily Tallies")))
    val arrayZipper = udf((m:Array[Int], n:Array[Int]) => m zip n)
    val dfxTogether = dfxWithOffset.select("*").withColumn("Yesterday_Today", arrayZipper(col("Daily Offset"),col("Daily Tallies")))//.drop(col("Daily Tallies")).drop(col("Daily Offset"))
    dfxTogether.collectAsList()
    //val increment = udf( (p:Array[(Int, Int)]) => p.map(q => q._2-q._1)) // <--
    //val dfxIncrement = dfxTogether.select(col("CR"),col("PS"), col("Yesterday_Today")).withColumn("Increment", increment(col("Yesterday_Today"))).drop(col("Yesterday_Today"))
    // val dfxIncrement = dfxTogether.select("*").withColumn("Increment", increment(col("Yesterday_Today"))).drop(col("Yesterday_Today")).collect
    //dfxIncrement.printSchema()
    //val dfxIncrement1 = dfxIncrement.collect
    //dfxIncrement1.show

    */

  }






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
