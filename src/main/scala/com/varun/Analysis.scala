package com.varun

import exceptions._
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{bround, udf}
import org.apache.spark.sql.{AnalysisException, DataFrame, SaveMode, SparkSession}

import scala.math.BigDecimal.RoundingMode
import scala.util.{Failure, Success, Try}

class Analysis {

  val spark: SparkSession =SparkSession.builder().master("local").appName("FlightAnalysis").enableHiveSupport().config("spark.sql.warehouse.dir", "C:\\tmp\\warehouse").getOrCreate()

    val file: DataFrame = spark.read.format("csv").option("inferSchema", value = true).option("header", value = true).load("C:\\Users\\Varun THK7\\Desktop\\test_df.csv")


  def nameToDataFrame(): Unit ={
    try {
    file.createOrReplaceTempView("flight")
    }
    catch{
      case ex:AnalysisException => sys.error("Table or View not found")
    }
  }


  def findDelayed(): Either[Throwable,Long] = {
      val df_delayedCount = Try(spark.sql("select * from flight where DEP_DELAY > 0"))
      df_delayedCount match {
        case Success(s) => Right(s.count())
        case Failure(f) => throw new ColumnNotFoundException()
      }
  }

  def findOnTime():Either[Throwable,Long] ={
    val df_OnTimeCount=Try(spark.sql("select * from flight where DEP_DELAY = 0 AND ARR_DELAY = 0 "))
    df_OnTimeCount match {
      case Success(s) => Right(s.count())
      case Failure(f) => throw new ColumnNotFoundException()
    }
  }

  def delayedWeeklyWise(): Either[Throwable,DataFrame] ={
     val weekDf= Try(spark.sql("select DAY_OF_WEEK,Count(*) as TotalDelayed from flight where DEP_DELAY > 0 GROUP BY DAY_OF_WEEK ORDER BY DAY_OF_WEEK")) match {
       case Success(s) => Right(s)
       case Failure(f) => Left(f)
     }
        weekDf
  }

  def onTimeWeeklyWise(): Either[Throwable,DataFrame] ={
      val onTimeweek=Try(spark.sql("select DAY_OF_WEEK,Count(*) as TotalOnTime from flight where DEP_DELAY = 0 AND ARR_DELAY=0 GROUP BY DAY_OF_WEEK ORDER BY DAY_OF_WEEK")) match {
        case Success(s) => Right(s)
        case Failure(f) => Left(f)
      }
        onTimeweek
  }

  def totalWeekly(): Either[Throwable,DataFrame] ={
    val totalWeek=Try(spark.sql("select DAY_OF_WEEK,Count(*) as TotalFlew from flight GROUP BY DAY_OF_WEEK ORDER BY DAY_OF_WEEK")) match {
      case Success(s) => Right(s)
      case Failure(f) => Left(f)
    }
        totalWeek
  }

  def toDoubleDigit(value:Double): Double ={
    BigDecimal(value).setScale(2,RoundingMode.HALF_UP).toDouble
  }

  def joinWeek(): Either[Throwable,DataFrame] ={
    val commonColName="DAY_OF_WEEK"
    val joinDelWithOntime=Try(delayedWeeklyWise().right.get.join(onTimeWeeklyWise().right.get, commonColName))
      joinDelWithOntime match {
        case Success(s) => Right(s)
        case Failure(f) => throw new JoinNotApplicableException()
      }
  }

  def joinTotalWithWeek(): Either[Throwable,DataFrame] ={
    val commonColName="DAY_OF_WEEK"
    val joinWeekTotal=Try(joinWeek().right.get.join(totalWeekly().right.get,commonColName ))
    joinWeekTotal match {
      case Success(s) => Right(s)
      case Failure(f) => throw  new JoinNotApplicableException()
    }
  }

  def joinWithPercent(): Either[Throwable,DataFrame] ={
    val joinTotal=joinTotalWithWeek().right.get
    val joinWithPercent = Try(joinTotal.withColumn("Delayed_Percentage", bround((joinTotal.col("TotalDelayed") / joinTotal.col("TotalFlew")) * 100, 2))
      .withColumn("OnTime_Percentage", bround((joinTotal.col("TotalOnTime") / joinTotal.col("TotalFlew")) * 100, 2)))
    joinWithPercent match {
      case Success(s) => Right(s)
      case Failure(f) => throw new DivideByZeroException("Cant Divide By Zero")
    }
  }

  def joinAllWeekly(): Either[Throwable,DataFrame]={
    val joinPerc=joinWithPercent().right.get
      val joinAll=Try(joinPerc.withColumn("Ratio", bround(joinPerc.col("Delayed_Percentage") / joinPerc.col("OnTime_Percentage")))) match {
        case Success(s) => Right(s)
        case Failure(f) => throw new DivideByZeroException("Cant Divide By Zero")
      }
        joinAll
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

  def addToHive(df:DataFrame): Boolean ={
    try {
    df.write.format("orc").mode(SaveMode.Overwrite).saveAsTable("Flight.flightData")
    true
    }
    catch {
      case ex:NoSuchDatabaseException => println("Database Does not Exist");false
    }
  }

}

object Analysis{
  def main(args: Array[String]): Unit = {
    val analysisClass=new Analysis()
    val initTime=System.currentTimeMillis()
    analysisClass.nameToDataFrame()
    val finalDataFrame=analysisClass.joinAllWeekly().right.get
    finalDataFrame.show()
    analysisClass.addToHive(finalDataFrame)
    println("ADDED TO HIVE")
    val endTime=System.currentTimeMillis()
    println((endTime-initTime)/1000)
  }
}
