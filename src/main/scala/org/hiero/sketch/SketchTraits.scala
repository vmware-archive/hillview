package org.hiero.sketch

/**
  * The Sketch interfaces/traits for Partial Datasets,
  * Maps and Sketches.
  */
trait IPDS[T] extends Serializable {
  def map[S] (iMap: IMap[T, S]) : IPDS[S]
  def sketch[R] (iSketch: ISketch[T, R]): R
  def print(): Unit
}

trait IMap[T, S] extends Serializable {
  def map(value: T) : S
}

trait ISketch[T, R] extends Serializable {
  def apply (data: T) : R
  def combine (data: List[R]) : R
}