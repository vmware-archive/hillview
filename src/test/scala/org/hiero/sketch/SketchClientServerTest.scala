package org.hiero.sketch

import akka.actor.{ActorSelection, ActorSystem, Props}
import akka.pattern.ask
import com.typesafe.config.ConfigFactory
import org.junit.Assert._
import org.junit.{BeforeClass, Test}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps


object SketchClientServerTest {
  var remoteActor: ActorSelection = _

  @BeforeClass def initialize() {
    // Initialize the server
    val serverConfig = ConfigFactory.parseString("""
    akka {
      extensions = ["com.romix.akka.serialization.kryo.KryoSerializationExtension$"]

      actor {
        serializers.java = "com.romix.akka.serialization.kryo.KryoSerializer"

        kryo {
          type = "nograph"
          idstrategy = "default"
          serializer-pool-size = 1024
          kryo-reference-map = false
        }
        provider = remote
      }
      serialization-bindings {
        "java.io.Serializable" = none
      }
      remote {
        enabled-transports = ["akka.remote.netty.tcp"]
        netty.tcp {
          hostname = "127.0.0.1"
          port = 2554
        }
      }
    }""")
    val serverSystem = ActorSystem("SketchApplication", ConfigFactory.load(serverConfig))
    val lpds = new LocalPDS(List(10,20,30,40))
    val serverActor = serverSystem.actorOf(Props(new SketchServerActor(lpds)), name = "ServerActor")


    //Initialize the client
    val clientConfig = ConfigFactory.parseString("""
    akka {
      extensions = ["com.romix.akka.serialization.kryo.KryoSerializationExtension$"]
      actor {
       serializers.java = "com.romix.akka.serialization.kryo.KryoSerializer"

       kryo {
         type = "nograph"
         idstrategy = "default"
         serializer-pool-size = 1024
         kryo-reference-map = false
       }
        provider = remote
      }
      serialization-bindings {
        "java.io.Serializable" = none
      }
      remote {
        enabled-transports = ["akka.remote.netty.tcp"]
        netty.tcp {
          hostname = "127.0.0.1"
          port = 2552
        }
      }
    }""")
    val clientSystem = ActorSystem("SketchApplication", ConfigFactory.load(clientConfig))
    remoteActor = clientSystem.actorSelection(
      "akka.tcp://SketchApplication@127.0.0.1:2554/user/ServerActor")
  }
}

class SketchClientServerTest {

  @Test def testRequestResponse() {
    val future  = (SketchClientServerTest.remoteActor ? new IncrementingMap)(5 seconds)
    val result = Await.result(future, 5 seconds).asInstanceOf[Int]
    assertEquals(0, result)
  }

  @Test def testSketch() {
    val futureSketch  = (SketchClientServerTest.remoteActor ? new SummationSketch)(5 seconds)
    val resultSketch = Await.result(futureSketch, 5 seconds).asInstanceOf[Int]
    assertEquals(100, resultSketch)
  }
}