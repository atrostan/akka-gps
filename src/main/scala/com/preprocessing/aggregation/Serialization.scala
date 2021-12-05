package com.preprocessing.aggregation

import com.preprocessing.aggregation.HDFSUtil.createHDFSDirIfNotExists
import com.preprocessing.partitioning.Util.triples
import org.apache.commons.lang.SerializationUtils
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.io.{ArrayWritable, Writable}
import org.apache.spark.sql.{Dataset, Row}

import java.io.{
  DataInput,
  DataOutput,
  FileInputStream,
  FileNotFoundException,
  FileOutputStream,
  IOException,
  ObjectInputStream,
  ObjectOutputStream
}
import scala.collection.mutable

object Serialization {
  class Main(
      val vid: Int,
      //      val pid: Int,
      val mirrors: Set[Int],
      val neighs: List[(Int, Int, Int)],
      val partitionInDegree: Int
  ) extends Serializable {
    override def toString() = {
      var s = ""
      s += s"vid: ${vid};\t"
      s += s"mirrors: ${mirrors};\t"
      s += s"ns: ${neighs};\t"
      s += s"pInDeg: ${partitionInDegree};"
      s
    }
  }

  class Mirror(
      val vid: Int,
      //      val pid: Int,
      val mainPid: Int,
      val neighs: List[(Int, Int, Int)],
      val partitionInDegree: Int
  ) extends Serializable {
    override def toString() = {
      var s = ""
      s += s"vid: ${vid};\t"
      s += s"main: ${mainPid};\t"
      s += s"ns: ${neighs};\t"
      s += s"pInDeg: ${partitionInDegree};"
      s
    }
  }

  def parseNeighsStr(s: String): List[(Int, Int, Int)] = {
    if (s.contains("List()")) {
      List()
    } else {
      val neighsStr = s
        .split("List\\(")(1)
        .split("\\)\\)")(0)
        .replace("(", "")
        .replace(")", "")
        .split(",")
        .map(_.trim)
        .toList
        .filter(s => s != "")
      triples(neighsStr.map(_.trim.toInt))
    }

  }

  def parseMirrorsStr(s: String): Set[Int] = {
    val mirrorPidsStr = s
      .split("Set\\(")(1)
      .split("\\)")(0)
      .split(",")
      .map(_.trim)
      .toList
      .filter(s => s != "")
    if (mirrorPidsStr.size > 0) {
      mirrorPidsStr.map(_.toInt).toSet
    } else {
      Set()
    }
  }

  def parseMainStr(s: String): (Int, Set[Int], List[(Int, Int, Int)], Int) = {
    val neighs = parseNeighsStr(s)
    val mrrs = parseMirrorsStr(s)
    val commaSplit = s.split(",")
    val vid = commaSplit(0)
      .replace("(", "")
      .trim
      .toInt
    val partitionInDegree = commaSplit.last
      .replace(")", "")
      .trim
      .toInt
    (vid, mrrs, neighs, partitionInDegree)
  }

  def parseMirrorStr(s: String): (Int, Int, List[(Int, Int, Int)], Int) = {
    val neighs = parseNeighsStr(s)
    val commaSplit = s.split(",")
    val vid = commaSplit(0)
      .replace("(", "")
      .trim
      .toInt
    val mainPid = commaSplit(1).trim.toInt
    val partitionInDegree = commaSplit.last
      .replace(")", "")
      .trim
      .toInt
    (vid, mainPid, neighs, partitionInDegree)
  }

  def readMirrorTextFile(path: String, fs: FileSystem) = {
    val p = new Path(path)
    val stream = fs.open(p)
    def readLines = Stream.cons(stream.readLine, Stream.continually(stream.readLine))
    //This example checks line for null and prints every existing line
    readLines
      .takeWhile(_ != null)
      .map { line =>
        parseMirrorStr(line)
      }
      .toArray
  }

  def readMainTextFile(path: String, fs: FileSystem) = {
    val p = new Path(path)
    val stream = fs.open(p)
    def readLines = Stream.cons(stream.readLine, Stream.continually(stream.readLine))
    //This example checks line for null and prints every existing line
    readLines
      .takeWhile(_ != null)
      .map { line =>
        parseMainStr(line)
      }
      .toArray
  }

  def readObjectArray[T](path: String) = {
    try {
      val fis = new FileInputStream(s"${path}")
      val ois = new ObjectInputStream(fis)
      val objArray: Array[T] = ois.readObject().asInstanceOf[Array[T]]
      ois.close()
      objArray
    } catch {
      case e: FileNotFoundException =>
        e.printStackTrace()
      case e: IOException =>
        e.printStackTrace()
      case e: ClassNotFoundException =>
        e.printStackTrace()
    }
  }

//
//  private implicit def arrayToArrayWritable[T <% Writable: scala.reflect.ClassTag](arr: Traversable[T]): ArrayWritable = {
//    def anyToWritable[U <% Writable](u: U): Writable = u
//
//    new ArrayWritable(scala.reflect.ClassTag[T].asInstanceOf[Class[Writable]],
//      arr.map(x => anyToWritable(x)).toArray)
//  }

  def writeMainArray(partition: Dataset[Row], path: String, fs: FileSystem) = {
    val mainArray = partition.rdd
      .map {
        case Row(
              vid: Int,
              pid: Int,
              pidsWithMirrors: mutable.WrappedArray[Int],
              neighs: mutable.WrappedArray[Int],
              partitionInDegree: Int
            ) =>
          (vid, pidsWithMirrors.toSet, triples(neighs.toList), partitionInDegree)
      }
      .coalesce(1)
      .saveAsTextFile(path)

//    try {
//      val fos = new FileOutputStream(s"${path}/mains.ser")
//      val oos = new ObjectOutputStream(fos)
//      oos.writeObject(mainArray)
//    } catch {
//      case e: FileNotFoundException =>
//        e.printStackTrace()
//      case e: IOException =>
//        e.printStackTrace()
//      case e: ClassNotFoundException =>
//        e.printStackTrace()
//    }
  }

  def writeMirrorArray(partition: Dataset[Row], path: String) = {
    val mirrorArray = partition.rdd
      .map {
        case Row(
              vid: Int,
              pid: Int,
              mainPid: Int,
              neighs: mutable.WrappedArray[Int],
              partitionInDegree: Int
            ) =>
          (vid, mainPid, triples(neighs.toList), partitionInDegree)
      }
      .coalesce(1)
      .saveAsTextFile(path)
//    try {
//      val fos = new FileOutputStream(s"${path}/mirrors.ser")
//      val oos = new ObjectOutputStream(fos)
//      oos.writeObject(mirrorArray)
//    } catch {
//      case e: FileNotFoundException =>
//        e.printStackTrace()
//      case e: IOException =>
//        e.printStackTrace()
//      case e: ClassNotFoundException =>
//        e.printStackTrace()
//    }
  }
}
