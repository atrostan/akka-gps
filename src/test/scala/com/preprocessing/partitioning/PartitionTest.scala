package com.preprocessing.partitioning

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

class PartitionTest extends FunSuite with Matchers with BeforeAndAfterAll{
  private var sparkConf: SparkConf = _
  private var sc: SparkContext = _

  override def beforeAll() {
    sparkConf = new SparkConf().setAppName("partitioning-unit-testing").setMaster("local")
    sc = new SparkContext(sparkConf)
  }

  // TODO;

  override def afterAll() {
    sc.stop()
  }
}
