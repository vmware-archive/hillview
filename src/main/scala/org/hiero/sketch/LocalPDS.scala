package org.hiero.sketch

/**
  * A Local Partial Data Set.
  */
class LocalPDS[A](value: A) extends IPDS[A] {
  var result:A = value
  override def map[T](mapper: IMap[A, T]): IPDS[T] = {
    val s = mapper.map(value)
    return new LocalPDS[T](s)
  }

  def print(): Unit = {
    println(result)
  }

  override def sketch[R](sketcher: ISketch[A, R]): R = {
    return sketcher.apply(result)
  }
}