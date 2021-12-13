package com.algorithm

case class Colour(num: Int) {
  require(num >= -1)
  override def toString() = {
    s"${num}"
  }
}

trait LocalMaximalColouringAbstractMode extends VertexProgram[Int, Int, Set[Int], Colour] {
  override def gather(edgeVal: Int, message: Int): Set[Int] = Set(message)

  override def sum(a: Set[Int], b: Set[Int]): Set[Int] = a.union(b)

  override def apply(
      superStepNumber: Int,
      thisVertex: VertexInfo,
      oldVal: Colour,
      total: Option[Set[Int]]
  ): Colour = {
    if (superStepNumber == 0) {
      Colour(-1)
    } else {
      oldVal match {
        case Colour(-1) => {
          total match {
            case None => {
              // Colour myself with superstep number
              Colour(superStepNumber - 1)
            }
            case Some(idSet) => {
              if (idSet.max < thisVertex.id) {
                // Colour myself with superstep number
                Colour(superStepNumber - 1)
              } else {
                Colour(-1)
              }
            }
          }
        }
        case c => c
      }
    }
  }

  override def scatter(
      superStepNumber: Int, 
      thisVertex: VertexInfo,
      oldVal: Colour,
      newVal: Colour
  ): Option[Int] = {
    newVal match {
      case Colour(-1) => Some(thisVertex.id)
      case c          => None
    }
  }

  override def voteToHalt(superStepNumber: Int, oldVal: Colour, newVal: Colour): Boolean = {
    newVal match {
      case Colour(-1) => false
      case c          => true
    }
  }

  override val defaultVertexValue: Colour = Colour(-1)

  override val defaultActivationStatus: Boolean = true
}

object LocalMaximaColouring extends LocalMaximalColouringAbstractMode {
  override val mode = VertexProgram.Outwards
}

object LocalMaximaColouringBidirectional extends LocalMaximalColouringAbstractMode {
  override val mode: VertexProgram.Mode = VertexProgram.Bidirectional
  
}