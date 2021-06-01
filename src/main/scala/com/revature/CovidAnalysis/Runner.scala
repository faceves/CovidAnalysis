package com.revature.CovidAnalysis

import loadpath.LoadPath
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.sql.{Column, Row}
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

    //loading data
    val covid_accum_DB = spark.read
      .option("header", true)
      .option("delimiter", ",")
      .format("csv")
      .load(LoadPath.hdfs_path + "covid_19_data.csv")
      .toDF()

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




    //starting first occurrence relationship
    val firstConfirmedCountries=
      dataCleanseFilter(
        firstOccurrenceCovid19(covid_accum_DB,"Confirmed","Country/Region")
          .select("First Confirmed Date","Country/Region","Confirmed"),
        "Country/Region"
    )

    val firstConfirmedUSStates =
      dataCleanseFilter(
        firstOccurrenceCovid19(covid_accum_DB,"Confirmed","Province/State")
          .where($"Country/Region" === "US")
          .select("First Confirmed Date","Province/State","Confirmed"),
        "Province/State"
      )

    val firstDeathsCountries =
      dataCleanseFilter(
        firstOccurrenceCovid19(covid_accum_DB, "Deaths", "Country/Region")
          .select("First Deaths Date", "Country/Region", "Deaths"),
        "Country/Region"
      )
    val firstDeathsUSStates =
      dataCleanseFilter(
        firstOccurrenceCovid19(covid_accum_DB, "Deaths", "Province/State")
          .where($"Country/Region" === "US")
          .select("First Deaths Date", "Province/State", "Deaths"),
        "Province/State"
      )

    val accumCountries = attainTargetAccum(covid_accum_DB,"Country/Region")
    val accumUSStates = attainTargetAccum(covid_accum_DB, "Province/State")


    displayLeastAndMostInfected(accumCountries, accumUSStates)
    displayFirstOccurenceRelationship(
      firstConfirmedCountries,
      firstDeathsCountries,
      firstConfirmedUSStates,
      firstDeathsUSStates,
      accumCountries,
      accumUSStates,
      covid_deaths_US_DB
    )

    //starting historic, increment, & respective percentages
    println("Historic Table")
    covid_confirmed_US_DB.show(1)


    println("Increment Table")
    val xyz = incrementGenerator(covid_confirmed_US_DB, true, spark)
    xyz.show(1)

    println("Show the Percent Change Between Days in Historic Data")
    percentChangeGenerator(covid_confirmed_US_DB, true, spark).show(1)

    println("Show the Percent Change Between Days in Incremental Data")
    percentChangeGenerator(xyz,true, spark).show(1)


    spark.close()
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
   * @return the first occurence data frame based on the occurrence type
   */

  def firstOccurrenceCovid19(covid19AccumDB: DataFrame, occurTypeCol: String, partitionByCol: String, partitionByTarget: String = ""): DataFrame = {

    //removes useless data and adding a formatted date column
    val filteredDF =
      covid19AccumDB.where(s"$occurTypeCol > 0")
        .withColumn("FormattedDate",to_date(covid19AccumDB("ObservationDate"), "MM/dd/yy"))
        .withColumnRenamed("ObservationDate", s"First $occurTypeCol Date")
        .cache()

    //partitioning by given column to order the dates and grab the first instance.
    val window = Window.partitionBy(partitionByCol).orderBy("FormattedDate")
    var firstOccurDF =
      filteredDF
        .withColumn("rowNum", row_number().over(window))
        .where("rowNum == 1" )
        .orderBy("FormattedDate")
        .drop("rowNum", "FormattedDate")
    if(partitionByTarget != "")
      firstOccurDF = firstOccurDF.where(firstOccurDF(partitionByCol).like("%"+partitionByTarget+"%"))

    filteredDF.unpersist()
    firstOccurDF
  }


  /**
   * firstOccurRelationship creates a dataset that attains the first occurence dates of first deaths and first confirmed
   * cases and compares it to 3 ratios: Case Fatality Ratio, Case Recovered Ratio and Mortality Ratio, to find a relationship
   * of how reactive/responsive states or countries are according to the timeframe(dates) given by the first confirmed or death case.
   * Needed the accumulated table of Confirmed, Deaths, and Recovered instances in order to calculate the 3 ratios.
   * Population is also needed from the time_series_covid_19_deaths_US.csv, so only the two columns of States and Population
   * are needed to be passed in to be joined.
   *
   * Case Fatality Ratio = Total Deaths / Total Confirmed
   * Case Recovered Ratio = Total Recovered / Total Confirmed
   * Mortality Rate = Total Deaths / Population
   * @param firstOccur = The first occurrence dataframe that has the joined DF's from First Deaths and First Confirmed
   *                   firstOccur dataframes
   * @param accumDF = The accumulated dataframe that has the summed data of Total Confirmed, Total Deaths, and Total Recovered
   * @param joinCols = The join columns condition that are required to join the firstOccur and the accumDF
   * @param populationDF = The dataframe required that contains the States Column with their respective Population Column
   * @return = Returns a dataframe consisting of the dates of the first occurrences of First Death and First Confirmed with
   *         the Case Fatality Ratio, Mortality Ratio, and Case Recovered Ratio to : compare amongst other entities, display
   *         and find a relation revolving around the first occurrence dates.
   */
  def firstOccurrRelationship(firstOccur :DataFrame, accumDF : DataFrame, joinCols: Seq[String], populationDF: DataFrame = null): DataFrame = {
    val firstJoined = accumDF.join(firstOccur,joinCols,"inner")

    //Case Fatality Rate = Total Deaths/ Total Confirmed
    val caseFatalityRatio =
      round(
        firstJoined("Total Deaths").cast(DoubleType)/(firstJoined("Total Confirmed").cast(DoubleType).cast(DoubleType)),
        2
        )

    var relationship =
      firstJoined
        .withColumn("Case Fatality Ratio", caseFatalityRatio)

    //if the occurenceRelationship is related to specifically the US states, populationDF is required to show mortality rate
    //populationDF is specific to the time_series_death_us.csv where it has the province state column as Province_State
    if(populationDF != null){
      relationship = relationship.join(populationDF,relationship("Province/State") === populationDF("Province_State"),"inner")

      // Mortality Ratio = Total Deaths/ Population
      val mortalityRatio =
        round(
          relationship("Total Deaths").cast(DoubleType) / relationship("Population").cast(DoubleType),
          2
        )
      relationship = relationship.withColumn("Mortality Ratio", mortalityRatio).drop("Province_State","Population")
    }

    //Case Recovery Ratio = Total Recovered/Total Confirmed
    val caseRecoveryRatio =
      round(
        relationship("Total Recovered").cast(DoubleType) / relationship("Total Confirmed"),
        2
      )
    relationship = relationship.withColumn("Case Recovery Ratio", caseRecoveryRatio)
    relationship.drop("Total Confirmed","Total Deaths","Total Recovered")
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

  /**
   * Displays the first occurence relationship in a dataset both for US States and Countrie by joining the respective
   * firstDeaths and firstConfirmed dataframes to pass it into the firstOccurrenceRelation function to return back a
   * dataset containing the columns to display and find a relationship between the first date and the ratios of Case
   * Fatality, Case Recovered, and Mortality. Comparison and Analysis between each entity is also possible (US State or
   * Country).
   *
   * Side Note: displayFirstOccurrenceRelationship is not ideal, it creates dataframes within the function that can
   * possibly be used outside of it. But for this projects scope it is fine. Otherwise it would have been passed in as a
   * parameter to be used outside of the display function.
   *
   * @param firstConfirmedCountries = first confirmed countries values consisting of the date with its respective confirmed case
   * @param firstDeathsCountries = first deatgs countries values consisting of the date with its respective deaths case
   * @param firstConfirmedUSStates = first confirmed US States values consisting of the date with its respective confirmed case
   * @param firstDeathsUSStates = first deaths countries values consisting of the date with its respective deaths case
   * @param accumCountries = accumulated values of Total Confirmed, Total Deaths, Total Recovered for Countries
   * @param accumUSStates = accumulated values of Total Confirmed, Total Deaths, Total Recovered for US States
   * @param covid_deaths_US_DB = the original source of covid deaths, to be used by passing in only the Population column
   *                           with its respective Province/State column into the firstOccurenceRelationship for US States.
   */
  def displayFirstOccurenceRelationship(
                                         firstConfirmedCountries: DataFrame,
                                         firstDeathsCountries: DataFrame,
                                         firstConfirmedUSStates: DataFrame,
                                         firstDeathsUSStates: DataFrame,
                                         accumCountries: DataFrame,
                                         accumUSStates: DataFrame,
                                         covid_deaths_US_DB: DataFrame
                                       ) : Unit = {
    val firstCountries = firstConfirmedCountries.join(firstDeathsCountries,"Country/Region")
    val firstCountriesRelationship =
      firstOccurrRelationship(
        firstCountries,
        accumCountries,
        Seq[String]("Country/Region")
      )

    //summing all counties into by its US  state.
    val statePopulationDF =
      dataCleanseFilter(
        covid_deaths_US_DB.select("Province_State", "Population"),
        "Province_State"
      )
        .groupBy("Province_State")
        .agg(sum(col("Population").cast(IntegerType)).as("Population"))

    val firstUSStates = firstConfirmedUSStates.join(firstDeathsUSStates,"Province/State")
    val firstUSStatesRelationship =
      firstOccurrRelationship(
        firstUSStates,
        accumUSStates,
        Seq[String]("Province/State"),
        statePopulationDF
      )

    //Reordering the columns for display
    println("FIrst Occurence Relationship for Countries:")
    firstCountriesRelationship
      .select(
        "First Confirmed Date",
        "First Deaths Date",
        "Country/Region",
        "Confirmed",
        "Deaths",
        "Case Fatality Ratio",
        "Case Recovery Ratio"
      ).show()

    println("FIrst Occurence Relationship for US States:")
    firstUSStatesRelationship
      .select(
        "First Confirmed Date",
        "First Deaths Date",
        "Province/State",
        "Confirmed",
        "Deaths",
        "Case Fatality Ratio",
        "Mortality Ratio",
        "Case Recovery Ratio"
      ).show()
  }

  /**
   * Displays the Top 10 Most and Least infected countries and US states.
   * @param accumCountries = accumulated values of Confirmed, Deaths and Recovered for Countries.
   * @param accumUSStates = accumulated values of Confirmed, Deaths and Recovered for US States.
   */
  def displayLeastAndMostInfected(accumCountries : DataFrame, accumUSStates:DataFrame): Unit = {
    println("Top 10 Most Infected Countries:")
    accumCountries
      .orderBy(desc("Total Confirmed"))
      .select(col("Country/Region"), col("Total Confirmed").as("Infected"))
      .show(10)

    println("Top 10 Least Infected Cpuntries:")
    accumCountries
      .orderBy(asc("Total Confirmed"))
      .select(col("Country/Region"), col("Total Confirmed").as("Infected"))
      .show(10)

    println("Top 10 Most Infected U.S States:")
    accumUSStates
      .orderBy(desc("Total Confirmed"))
      .select(col("Province/State"), col("Total Confirmed").as("Infected"))
      .show(10)

    println("Top 10 Least Infected U.S States:")
    accumUSStates
      .orderBy(asc("Total Confirmed"))
      .select(col("Province/State"), col("Total Confirmed").as("Infected"))
      .show(10)
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

  //DO NOT RUN THESE FUNCTIONS on pseudo standalone evironemnt, super expensive and time consuming

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

}
