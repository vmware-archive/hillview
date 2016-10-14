/**
  * The Sketch interfaces/traits for Partial Datasets,
  * Maps and Sketches.
  */
trait IPDS[T] {
  def map[S] (iMap: IMap[T, S]) : IPDS[S]
  def sketch[R] (iSketch: ISketch[T, R]): R
  def print(): Unit
}

trait IMap[T, S] {
  def map(value: T) : S
}

trait ISketch[T, R] {
  def apply (data: T) : R
  def combine (data: List[R]) : R
}