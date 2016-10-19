package org.hiero.sketch

import java.io.Serializable

/**
  * The core Sketch API,
  */

trait ProgressReporter[T] {
    def report(done: Long, outOf: Long, partialResult: T)
}

trait IPDS[T] {
    def map[S](iMap: IMap[T, S]): IPDS[S]

    def sketch[R](iSketch: ISketch[T, R]): R

    /* TODO
    def zip[S](other: IPDS[S]) : IPDS[(T, S)]
     */
    def print(): Unit
}

trait IMap[T, S] extends Serializable {
    def map(value: T): S
}

trait ISketch[T, R] extends Serializable {
    def apply(data: T): R

    def combine(data: List[R]): R
}