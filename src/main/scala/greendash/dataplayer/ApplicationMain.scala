package greendash.dataplayer

import akka.actor.ActorSystem
import greendash.dataplayer.Clock.Start

object ApplicationMain extends App {
    val system = ActorSystem("DataPlayerSystem")
    val clock = system.actorOf(Clock.props())
    clock ! Start

    system.awaitTermination()
}
