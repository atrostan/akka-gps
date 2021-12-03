package com.preprocessing.edgeList.compression

import com.Typedefs.EitherEdgeRDD
import com.preprocessing.edgeList.Compressor
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._

class CompressorTest extends FunSuite with Matchers with BeforeAndAfterAll {

  private var sparkConf: SparkConf = _
  private var sc: SparkContext = _

  override def beforeAll() {
    sparkConf = new SparkConf().setAppName("compression-unit-testing").setMaster("local")
    sc = new SparkContext(sparkConf)
  }

  def readEdgeList(infile: String, sep: String, isWeighted: Boolean): EitherEdgeRDD = {
    val strList: RDD[String] = sc.textFile(infile)
    val split = strList
      .filter(s => !s.contains("#"))
      .map(s => s.split(sep))
    if (isWeighted) {
      Left(
        split
          .map(s => ((s(0).toInt, s(1).toInt), s(2).toInt))
          .reduceByKey(
            math.max(_, _)
          ) // if multiple edges between same source and dest, take the maximum weighted edge
          .map(row => (row._1._1, row._1._2, row._2)) // unpack again
          .distinct()
          .filter(e => e._1 != e._2)
          .sortBy(e => (e._1, e._2, e._3))
          .zipWithIndex()
          .map(r => (r._2, r._1))
      )
    } else {
      Right(
        split
          .map(s => (s(0).toInt, s(1).toInt))
          .distinct()
          .filter(e => e._1 != e._2)
          .sortBy(e => (e._1, e._2))
          .zipWithIndex()
          .map(r => (r._2, r._1))
      )
    }
  }

  test(
    "Correct compression of shuffled, duplicated, weighted edge list with gaps in the vertex id space"
  ) {
    // dup: 38 49 134
    val inputFile = "src/test/resources/graphs/rmat/wt_shuffled_64_64.net"
    val groundTruthFile = "src/test/resources/graphs/rmat/groundTruth/compressed_rmat_64_64.net"
    val sep = " "
    val isWeighted = true
    val edgeList = readEdgeList(inputFile, sep, isWeighted)

    val c = new Compressor(edgeList)
    val compressed = c.compress()
    compressed match {
      case Left(compressed) =>
        val groundTruth: RDD[String] = sc.textFile(groundTruthFile)
        val split = groundTruth
          .filter(s => !s.contains("#"))
          .map(s => s.split(sep))
          .map(s => (s(0).toLong, (s(1).toInt, s(2).toInt, s(3).toInt)))

        val a1 = split.collect()
        val a2 = compressed.collect()
        a1.sameElements(a2)
      case Right(_) => ???
    }
  }

  test(
    "Correct compression of shuffled, duplicated, unweighted edge list with gaps in the vertex id space"
  ) {
    // dup: 38 49 134
    val inputFile = "src/test/resources/graphs/rmat/shuffled_64_64.net"
    val groundTruthFile = "src/test/resources/graphs/rmat/groundTruth/compressed_rmat_64_64.net"
    val sep = " "
    val isWeighted = false
    val edgeList = readEdgeList(inputFile, sep, isWeighted)
    val c = new Compressor(edgeList)
    val compressed = c.compress()
    compressed match {
      case Right(compressed) =>
        val groundTruth: RDD[String] = sc.textFile(groundTruthFile)
        val split = groundTruth
          .filter(s => !s.contains("#"))
          .map(s => s.split(sep))
          .map(s => (s(0).toLong, (s(1).toInt, s(2).toInt)))
        val a1 = split.collect()
        val a2 = compressed.collect()
        a1.sameElements(a2)
      case Left(_) => ???
    }
  }

  override def afterAll() {
    sc.stop()
  }

}
