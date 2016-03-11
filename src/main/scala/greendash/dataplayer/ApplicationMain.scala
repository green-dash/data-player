package greendash.dataplayer

import akka.actor.ActorSystem
import akka.io.IO
import com.typesafe.config.ConfigFactory
import greendash.dataplayer.Clock.Start
import spray.can.Http

object ApplicationMain extends App {
    implicit val system = ActorSystem("DataPlayerSystem")
    val clock = system.actorOf(Clock.props())
    clock ! Start

    val config = ConfigFactory.load()
    var restPort = config.getInt("rest.port")
    val handler = system.actorOf(RestService.props(clock))
    IO(Http) ! Http.Bind(handler, interface = "localhost", port = restPort)

    system.awaitTermination()
}
