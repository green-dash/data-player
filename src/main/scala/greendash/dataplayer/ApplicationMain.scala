package greendash.dataplayer

import java.io.File

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import greendash.dataplayer.Clock.Continue

object ApplicationMain extends App {
    val system = ActorSystem("MyActorSystem")

    val dir = ConfigFactory.load().getString("fileFolder")
    val flist = getListOfFiles(dir)

    val clock = system.actorOf(Clock.props(flist))

    clock ! Continue

    system.awaitTermination()

    def getListOfFiles(dir: String):List[String] = {
        val d = new File(dir)
        d.listFiles.filter(_.isFile).map(_.getCanonicalPath).toList
    }

}
