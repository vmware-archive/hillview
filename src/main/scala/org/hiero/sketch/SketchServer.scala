import akka.actor.Actor

/**
  * Created by lsuresh on 10/13/16.
  */
class SketchServerActor[T](localPDS: LocalPDS[T]) extends Actor {
    def receive = {
      case mapper: IMap[T, Any] => {
        localPDS.map (mapper).print()
        sender ! 0
      }
      case sketcher: ISketch[T, Any] => {
        sender ! localPDS.sketch (sketcher)
      }
    }
}
