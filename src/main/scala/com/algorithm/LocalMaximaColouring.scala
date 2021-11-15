package com.algorithm

case class Colour(num: Int) {
  require(num >= 0)
}

object LocalMaximaColouring extends VertexProgram[Int, Int, Int, Set[Int], Option[Colour]] {

  override def gather(edgeVal: Int, message: Int): Set[Int] = Set(message)

  override def sum(a: Set[Int], b: Set[Int]): Set[Int] = a.union(b)

  override def apply(
      superStepNumber: Int,
      thisVertexId: Int,
      oldVal: Option[Colour],
      total: Option[Set[Int]]
  ): Option[Colour] = {
    if (superStepNumber == 0) {
      None
    } else {
      oldVal match {
        case Some(colour) => oldVal
        case None => {
          total match {
            case None => {
              // Colour myself with superstep number
              Some(Colour(superStepNumber - 1))
            }
            case Some(idSet) => {
              if (idSet.max < thisVertexId) {
                // Colour myself with superstep number
                Some(Colour(superStepNumber - 1))
              } else {
                None
              }
            }
          }
        }
      }
    }
  }

  override def scatter(
      thisVertexId: Int,
      oldVal: Option[Colour],
      newVal: Option[Colour]
  ): Option[Int] = {
    newVal match {
      case None         => Some(thisVertexId)
      case Some(colour) => None
    }
  }

  override def voteToHalt(oldVal: Option[Colour], newVal: Option[Colour]): Boolean = {
    newVal match {
      case None         => false
      case Some(colour) => true
    }
  }

  override val defaultVertexValue: Option[Colour] = None

  override val defaultActivationStatus: Boolean = true
}
