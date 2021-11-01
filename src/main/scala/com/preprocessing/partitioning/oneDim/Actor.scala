package com.preprocessing.partitioning.oneDim

import scala.collection.mutable.ArrayBuffer

trait Actor {
  val id: Int
  //TODO; entityRef's custom .hashCode() should map to the correct shard?
  var entityRef: Int = -1
  val partition: Partition
}

// main actor
class Main(val id: Int, p: Partition) extends Actor {
  var neighbors = ArrayBuffer[Actor]()
  var mirrors = ArrayBuffer[Mirror]()
  val partition = p

  override def toString(): String = {
    var s: String = ""
    s += s"Vertex ${id} Main on Partition ${partition.id}"
    s
  }
}

object Main {
  def apply(vid: Int, p: Partition): Main = {
    val m = new Main(vid, p)
    m
  }
}

// mirror actor
class Mirror(val id: Int, m: Main, p: Partition) extends Actor {
  // TODO; in 1D partitioning, a mirror will _only_ be aware of its main?
  val main: Main = m
  val partition = p

  override def toString(): String = {
    var s: String = ""
    s += s"Vertex ${id} Mirror on Partition ${partition.id} with main: ${main}"
    s
  }
}

object Mirror {
  def apply(vid: Int, main: Main, p: Partition): Mirror = {
    val mirror = new Mirror(vid, main, p)
    mirror
  }
}
