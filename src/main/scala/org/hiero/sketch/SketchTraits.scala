package org.hiero.sketch

import java.io.Serializable

/**
  * The core Sketch API,
  */

trait IPDS[T] {
    def map[S](iMap: IMapp[T, S]): IPDS[S]

    def sketch[R](iSketch: ISketchh[T, R]): R

    /* TODO
    def zip[S](other: IPDS[S]) : IPDS[(T, S)]
     */
    def print(): Unit
}

trait IMapp[T, S] extends Serializable {
    def map(value: T): S
}

trait ISketchh[T, R] extends Serializable {
    def apply(data: T): R

    def combine(data: List[R]): R
}