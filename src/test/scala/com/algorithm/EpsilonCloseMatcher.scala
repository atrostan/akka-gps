package com.algorithm

import org.scalatest.matchers.MatchResult
import org.scalatest.matchers.Matcher

object EpsilonCloseMatching {

  class EpsilonCloseCheck[T](epsilon: Double, expected: Map[T, Double]) extends Matcher[Map[T, Double]] {

    def apply(left: Map[T, Double]): MatchResult = {

      val bad = expected.filter { case (k, v) => {
        left.get(k) match {
          case None => false
          case Some(v_prime) => Math.abs(v - v_prime) > epsilon
        }
      }}

      MatchResult(
        left.keySet == expected.keySet && bad.isEmpty,
        s"Map ${left} not sufficiently close to Map ${expected}",
        "Maps sufficiently close"
      )
    }

  }
  
  def BeEpsilonClose[T](epsilon: Double)(expected: Map[T, Double]): Matcher[Map[T, Double]] = new EpsilonCloseCheck(epsilon, expected)

}


