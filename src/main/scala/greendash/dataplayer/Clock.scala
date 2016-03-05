package greendash.dataplayer

import java.util.Calendar

import akka.actor._
import com.typesafe.config.ConfigFactory
import greendash.dataplayer.Reader.NextLine

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._


class Clock(files: List[String]) extends Actor with ActorLogging {

    import Clock._

    val messageMap = mutable.Map[ActorRef, Message]()

    val runStart = Calendar.getInstance.getTimeInMillis

    log.info("run started")

    var expected = files.length
    var received = 0
    var lastTimestamp = 0L
    val speedFactor = ConfigFactory.load().getInt("speedFactor")

    val scheduler = context.system.scheduler

    files.foreach { fname =>
        val ref = context.actorOf(Reader.props(fname, self))
        context.watch(ref)
    }

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
                context.system.shutdown()
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
    }

    def publish(message: Message) = {
        // log.info(message.toString)
        KafkaBroker.send(message.topic, s"""{ "ts": ${message.timestamp}, "value": ${message.value} }""")
    }

    def hold(timestamp: Long) = {
        // first time
        if (lastTimestamp == 0) {
            lastTimestamp = timestamp
            self ! Continue
        }
        else {
            val delta = timestamp - lastTimestamp
            val duration = (delta.toDouble / speedFactor) * 1000
            // log.info(s"duration: $duration")
            lastTimestamp = timestamp
            Thread.sleep(0, duration.toInt)
            self ! Continue
            // val sleep = Duration(duration.toLong, NANOSECONDS)
            // scheduler.scheduleOnce(sleep, self, Continue)
        }
    }
}

object Clock {
    def props(files: List[String]) = Props(new Clock(files))
    class EmptyMessage()
    case class Message(topic: String, timestamp: Long, value: Double) extends EmptyMessage
    case object Continue
}

