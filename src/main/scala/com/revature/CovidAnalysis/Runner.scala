package com.revature.CovidAnalysis

import org.apache.spark.sql.SparkSession

object Runner {

  def main(args: Array[String]) : Unit = {
    val spark = SparkSession
      .builder()
      .appName("Covid Analysis")
      .master("local[*]")
      .getOrCreate()
  }

}
