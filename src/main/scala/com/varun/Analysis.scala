package com.varun

import exceptions.{TableNotFoundException, UnknownFormatException}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{bround, udf}
import org.apache.spark.sql.{AnalysisException, DataFrame, SaveMode, SparkSession}
import scala.io.StdIn.readLine

import scala.math.BigDecimal.RoundingMode

object Analysis {

  val spark: SparkSession =SparkSession.builder().master("local").appName("FlightAnalysis").enableHiveSupport().config("spark.sql.warehouse.dir", "C:\\tmp\\warehouse").getOrCreate()

    val file: DataFrame = spark.read.format("csv").option("inferSchema", value = true).option("header", value = true).load("C:\\Users\\Varun THK7\\Desktop\\test_df.csv")


  def nameToDataFrame(): Unit ={
    file.createOrReplaceTempView("flight")
  }


  def findDelayed(query:String): Long = {
    try {
      val df_delayedCount = spark.sql(query)
      df_delayedCount.count()
    }
    catch {
      case ex: AnalysisException => {
        ex.getMessage()
        println("Enter CorrectTable Name: \n")
        val newTableName = readLine()
        val query = "select * from " + newTableName + " where DEP_DELAY > 0"
        findDelayed(query)
      }
    }
  }

  def findOnTime(): Long ={
    val df_OnTimeCount=spark.sql("select * from flight where DEP_DELAY = 0 AND ARR_DELAY = 0 ")
    df_OnTimeCount.count()
  }

  def delayedWeeklyWise(): DataFrame ={
      spark.sql("select DAY_OF_WEEK,Count(*) as TotalDelayed from flight where DEP_DELAY > 0 GROUP BY DAY_OF_WEEK ORDER BY DAY_OF_WEEK")
  }

  def onTimeWeeklyWise(): DataFrame ={
      spark.sql("select DAY_OF_WEEK,Count(*) as TotalOnTime from flight where DEP_DELAY = 0 AND ARR_DELAY=0 GROUP BY DAY_OF_WEEK ORDER BY DAY_OF_WEEK")
  }

  def totalWeekly(): DataFrame ={
    spark.sql("select DAY_OF_WEEK,Count(*) as TotalFlew from flight GROUP BY DAY_OF_WEEK ORDER BY DAY_OF_WEEK")
  }

  def toDoubleDigit(value:Double): Double ={
    BigDecimal(value).setScale(2,RoundingMode.HALF_UP).toDouble
  }

  def joinAllWeekly(): DataFrame={
    try {
      val joinWeek = delayedWeeklyWise().join(onTimeWeeklyWise(), "DAY_OF_WEEK1")
      val joinTotalWithWeek = joinWeek.join(totalWeekly(), "DAY_OF_WEEK")
      val joinWithPercent = joinTotalWithWeek.withColumn("Delayed_Percentage", bround((joinTotalWithWeek("TotalDelayed") / joinTotalWithWeek("TotalFlew")) * 100, 2))
        .withColumn("OnTime_Percentage", bround((joinTotalWithWeek("TotalOnTime") / joinTotalWithWeek("TotalFlew")) * 100, 2))
      joinWithPercent.withColumn("Ratio", bround(joinWithPercent("Delayed_Percentage") / joinWithPercent("OnTime_Percentage")))
    }
    catch {
      case ex:AnalysisException => ex.getMessage()
    }
  }

  val toTimeUDF: UserDefinedFunction = udf((time:String) => time.length match {
    case 3 => "0"+time.charAt(0).toString+":"+time.substring(1)
    case 4 => time.substring(0,2)+":"+time.substring(2)
    case _ => throw new UnknownFormatException()
  })

  def testDate(): DataFrame ={
    val dfTime=spark.sql("select DEP_TIME from flight")
    dfTime.withColumn("FormattedTime",toTimeUDF(dfTime("DEP_TIME")))
  }

  def filterNull(): DataFrame={
    spark.sql("select * from flight where DEP_TIME='NA'")
  }

  def addToHive(df:DataFrame): Unit ={
    df.write.format("orc").mode(SaveMode.Overwrite).saveAsTable("Flight.flightData")
  }

  def main(args: Array[String]): Unit = {
    val initTime=System.currentTimeMillis()
    nameToDataFrame()
    findDelayed("select * from flight where DEP_DELAY > 0")
    println("Control Here")
    val finalDataFrame=joinAllWeekly()
    addToHive(finalDataFrame)
    println("ADDED TO HIVE")
    val endTime=System.currentTimeMillis()
    println((endTime-initTime)/1000)
  }
}
