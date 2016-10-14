import java.io.Serializable
import scala.collection.mutable.ListBuffer

/**
  * An example Map function that can be issued
  * to a server
  */
class IncrementingMap extends IMap[List[Int], List[Int]] with Serializable {
  override def map(value: List[Int]): List[Int] = {
    val out = new ListBuffer[Int]()
    for (i <- value) {
      out += i + 1
    }
    return out.toList
  }
}

class SummationSketch extends ISketch[List[Int], Int] with Serializable {
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
