package com.epam.training.spark.core

import java.time.LocalDate

import com.epam.training.spark.core.domain.Climate
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Homework {
  val DELIMITER = ";"
  val RAW_BUDAPEST_DATA = "data/budapest_daily_1901-2010.csv"
  val OUTPUT_DUR = "output"

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("EPAM BigData training Spark Core homework")
      .setIfMissing("spark.master", "local[2]")
      .setIfMissing("spark.sql.shuffle.partitions", "10")
    val sc = new SparkContext(sparkConf)

    processData(sc)

    sc.stop()

  }

  def processData(sc: SparkContext): Unit = {

    /**
      * Task 1
      * Read raw data from provided file, remove header, split rows by delimiter
      */
    val rawData: RDD[List[String]] = getRawDataWithoutHeader(sc, Homework.RAW_BUDAPEST_DATA)

    /**
      * Task 2
      * Find errors or missing values in the data
      */
    val errors: List[Int] = findErrors(rawData)
    println(errors)

    /**
      * Task 3
      * Map raw data to Climate type
      */
    val climateRdd: RDD[Climate] = mapToClimate(rawData)

    /**
      * Task 4
      * List average temperature for a given day in every year
      */
    val averageTemeperatureRdd: RDD[Double] = averageTemperature(climateRdd, 1, 2)

    /**
      * Task 5
      * Predict temperature based on mean temperature for every year including 1 day before and after
      * For the given month 1 and day 2 (2nd January) include days 1st January and 3rd January in the calculation
      */
    val predictedTemperature: Double = predictTemperature(climateRdd, 1, 2)
    println(s"Predicted temperature: $predictedTemperature")

  }

  def getRawDataWithoutHeader(sc: SparkContext, rawDataPath: String): RDD[List[String]] =
    sc.textFile(rawDataPath)
      .filter(!_.startsWith("#"))
      .map(_.split(";", 7).toList)

  def findErrors(rawData: RDD[List[String]]): List[Int] =
    rawData.
      map( _.map ( s => if ( s.isEmpty ) 1 else 0 ) ).
      reduce((a,b) => (a,b).zipped.map(_+_))

  def mapToClimate(rawData: RDD[List[String]]): RDD[Climate] =
    rawData.map(r => Climate(r.head,r(1),r(2),r(3),r(4),r(5),r(6)))

  def averageTemperature(climateData: RDD[Climate], month: Int, dayOfMonth: Int): RDD[Double] =
    climateData.
      filter(c => c.observationDate.getMonthValue == month && c.observationDate.getDayOfMonth == dayOfMonth).
      map( _.meanTemperature.value )

  def predictTemperature(climateData: RDD[Climate], month: Int, dayOfMonth: Int): Double = {
    val d = LocalDate.of(2017,month,dayOfMonth)
    val reduced = avgTempWithCounter(climateData,d).
      union(avgTempWithCounter(climateData,d.minusDays(1))).
      union(avgTempWithCounter(climateData,d.plusDays(1))).
      reduce( (a, b) => (a._1 + b._1, a._2 + b._2) )

    reduced._1/reduced._2
  }

  def avgTempWithCounter(climateData: RDD[Climate], d: LocalDate): RDD[(Double, Int)] =
    averageTemperature(climateData,d.getMonthValue, d.getDayOfMonth).map( (_,1) )
}