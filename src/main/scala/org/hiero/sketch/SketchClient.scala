package org.hiero.sketch

import scala.collection.mutable.ListBuffer

/**
  * An example Map function that can be issued
  * to a server. It operates on a list of integers,
  * and increments them by 1.
  */
class IncrementingMap extends IMap[List[Int], List[Int]] {
    override def map(value: List[Int]): List[Int] = {
        val out = new ListBuffer[Int]()
        for (i <- value) {
            out += i + 1
        }
        return out.toList
    }
}

/**
  * An example Sketch function that can be issued
  * to a server. It sums up a list of integers.
  */
class SummationSketch extends ISketch[List[Int], Int] {
    override def apply(data: List[Int]): Int = {
        var total = 0
        for (i <- data) {
            total += i
        }
        return total
    }

    override def combine(data: List[Int]): Int = {
        return apply(data)
    }
}

object SketchClient extends App {
    println("Hello World")
}
