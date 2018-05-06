package com.varun

import org.apache.spark.sql.AnalysisException
import org.scalatest.FunSuite

class AnalysisTestSuite extends FunSuite{
  val analysis = new Analysis()
//  val analysisMocked=Mock[Analysis]

  def isViewDefined: Boolean ={
    try {
      analysis.nameToDataFrame()
      true
    }
    catch {
      case ex:AnalysisException => false
    }
  }


  test("test to check whether view can be named or not"){
    assert(isViewDefined)
  }

  test("test to find the delayed flights"){
    assert(analysis.findDelayed().isRight)
  }

  test("test to find the onTime flights"){
    assert(analysis.findOnTime().isRight)
  }

  test("test to find the flights that were delayed weekly wise"){
    assert(analysis.delayedWeeklyWise().isRight)
  }

  test("test to find onTime weekly wise flights"){
   assert(analysis.onTimeWeeklyWise().isRight)
  }

  test("test to find total flights week wise"){
    assert(analysis.totalWeekly().isRight)
  }

  test("test to check whether join was successful on week or nor"){
    assert(analysis.joinWeek().isRight)
  }

  test("test to join Total with week"){
    assert(analysis.joinTotalWithWeek().isRight)
  }

  test("test to join percentage"){
    assert(analysis.joinWithPercent().isRight)
  }

  test("test to check whether join was successful on All weekly or nor"){
    assert(analysis.joinAllWeekly().isRight)
  }

  test("test whether added to hive or not"){
    val dataframe=analysis.joinAllWeekly().right.get
    assert(analysis.addToHive(dataframe))
  }

}
