package com.preprocessing.partitioning.oneDim

import com.graph.{Edge, Vertex}

import scala.collection.mutable.ArrayBuffer

class Partitioning(P: Int, n: Int, m: Int) {

  // TODO; this may need to become a distributed array
  val mainArray = new Array[Main](n)
  val nPartitions: Int = P
  val nNodes: Int = n
  val nEdges: Int = m
  var partitions: ArrayBuffer[Partition] = ArrayBuffer[Partition]()

  def replicationFactor(): Double = {
    // (all mirrors) / nNodes
    // http://www.vldb.org/pvldb/vol12/p321-gill.pdf: "average number of proxies per node"
    // TODO; unsure if to include mains in the calculation; currently exclude
    // i.e. (all mirrors + all mains) / nNodes
     mainArray.map(m => m.mirrors.size).sum.toFloat / nNodes
  }

  def partitionEdges(es: ArrayBuffer[Edge]): Unit = {
    for (e <- es) {
      // assign edges to partitions
      this.assign(e)
      val src: Vertex = e.source
      val dest: Vertex = e.dest

      // create a main actor (reference) for src, dest, if doesn't exist already
      for (v: Vertex <- Seq(src, dest)) {
        if (mainArray(v.id) == null) {
          mainArray(v.id) = Main(v.id, getMainPartition(v))
        }
      }
    }
  }

  // in 1D partitioning by source, a vertex v's main copy will be stored in the
  // partition id p_i,
  // where, v.id % nPartitions == p_i
  // i.e. store a vertex in the partition with all of its outgoing edges
  // 1d edge partitioning ~=? vertex partitioning
  // TODO; parameterize partitioning by source/destination
  def getMainPartition(v: Vertex): Partition = get(v.id % nPartitions)
  def get(pid: Int): Partition = partitions(pid)

  def assign(e: Edge): Unit = {
    val src: Vertex = e.source
    val partitionToAssign: Int = src.id % nPartitions
    this.get(partitionToAssign).edges += e
  }

  // set up an empty edge list container per partition
  def init(): Unit = {
    for (pid <- 0 until nPartitions) {
      partitions += Partition(pid)
    }
  }

  def assignMainsMirrors(): Unit = {
    for (partition <- partitions) {
      for (edge <- partition.edges) {
        val src: Vertex = edge.source
        val dest: Vertex = edge.dest
        val srcMain: Main = mainArray(src.id)

        if (getMainPartition(dest) == partition) { // dest is a main in this partition
          val destRef: Main = mainArray(dest.id)
          srcMain.neighbors += destRef
        } else { // dest is a mirror in this partition
          if (partition.mirrorMap.isDefinedAt(dest.id)) { // already created a mirror for dest in this partition
            val destRef: Mirror = partition.mirrorMap.get(dest.id).get
            srcMain.neighbors += destRef
          } else { // need to create a mirror for dest in this partition
            // get the main reference for this mirror
            val destMain: Main = mainArray(dest.id)
            val destRef: Mirror = Mirror(
              dest.id,
              destMain, // mirror -> main hook
              partition
            )
            destMain.mirrors += destRef // main -> mirror hook
            partition.mirrorMap += (dest.id -> destRef)
            srcMain.neighbors += destRef
          }
        }
      }
    }
  }

  override def toString(): String = {
    var s: String = ""
    for (p <- partitions) {
      s += s"Partition ${p.id}:\n"
      s += "\tEdges:"
      for (e <- p.edges) {
        val src: Vertex = e.source
        val dest: Vertex = e.dest
        s += s"(${src.id}, ${dest.id}), "
      }
      s += "\n"
    }
    s
  }
}

object Partitioning {
  def apply(P: Int, es: ArrayBuffer[Edge], n: Int, m: Int): Partitioning = {
    val png = new Partitioning(P, n, m)
    png.init()
    png.partitionEdges(es)
    png.assignMainsMirrors()

    png
  }
}