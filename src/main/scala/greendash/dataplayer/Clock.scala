package greendash.dataplayer

import java.io.File
import java.util.Calendar

import akka.actor._
import com.typesafe.config.ConfigFactory
import greendash.dataplayer.Reader.{Message, NextLine}

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._


class Clock() extends Actor with ActorLogging {

    import Clock._

    val messageMap = mutable.Map[ActorRef, Message]()

    val runStart = Calendar.getInstance.getTimeInMillis

    var expected = 0
    var received = 0
    var lastTimestamp = 0L
    val speedFactor = ConfigFactory.load().getInt("speed.factor")
    val scheduler = context.system.scheduler

    override def receive = {

        case message: Message =>
            messageMap(sender) = message
            received = received + 1

        case Terminated(reader) =>
            messageMap -= reader
            expected = expected - 1
            if (messageMap.isEmpty) {
                log.info("all readers finished: stopping clock")
                val t = Calendar.getInstance.getTimeInMillis - runStart
                log.info("run took {} milliseconds", t.toString)

                if (ConfigFactory.load().getBoolean("stream.repeat")) {
                    start()
                } else {
                    context.system.shutdown()
                }
            }

        case Continue if received > expected =>
            log.error(s"received > expected: $received > $expected")
            context.stop(self)

        case Continue if received < expected =>
            // wait until messageMap is fully populated
            self ! Continue

        case Continue if received == expected =>
            val (reader, message) = messageMap.minBy(_._2.timestamp)
            publish(message)
            received = received - 1
            reader ! NextLine
            hold(message.timestamp)

        case Start => start()
    }

    def start() = {
        log.info("run started")
        val dir = ConfigFactory.load().getString("file.folder")
        val files = getListOfFiles(dir)

        expected = files.length
        received = 0
        lastTimestamp = 0L

        files.foreach { fname =>
            val ref = context.actorOf(Reader.props(fname, self))
            context.watch(ref)
        }
        self ! Continue
    }

    def publish(message: Message) = {
        // log.info(message.toString)
        KafkaBroker.send(message.topic, s"""{ "ts": ${message.timestamp}, "value": ${message.value} }""")
    }

    def hold(timestamp: Long) = {
        // first time
        if (lastTimestamp == 0 || speedFactor == 0) {
            lastTimestamp = timestamp
            self ! Continue
        }
        else {
            val delta = timestamp - lastTimestamp
            lastTimestamp = timestamp

            val duration = delta / speedFactor
            val sleep = Duration(duration, MILLISECONDS)
            scheduler.scheduleOnce(sleep, self, Continue)
        }
    }

    def getListOfFiles(dir: String):List[String] = {
        val d = new File(dir)
        d.listFiles.filter(_.isFile).map(_.getCanonicalPath).toList
    }

}

object Clock {
    def props() = Props(new Clock())
    case object Continue
    case object Start
}

