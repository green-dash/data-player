package greendash.dataplayer

import java.io.File
import java.util.Calendar

import akka.actor._
import com.typesafe.config.ConfigFactory
import greendash.dataplayer.Reader.{EmptyMessage, Message, NextLine}
import greendash.dataplayer.model.{FileInfo, MetaDataReader}

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._


class Clock() extends Actor with ActorLogging {

    import Clock._

    val config = ConfigFactory.load()
    var speedFactor = config.getInt("speed.factor")
    val topic = config.getString("kafka.publisher.topic")

    val messageMap = mutable.Map[ActorRef, Message]()

    val runStart = Calendar.getInstance.getTimeInMillis

    var expected = 0
    var received = 0
    var lastTimestamp = 0L
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

        case _: EmptyMessage =>
            /* empty message returned: there was no value in the line read, so continue */
            sender ! NextLine

        case AdjustSpeed(i) =>
            log.info("setting speedFactor to: {}", i)
            speedFactor = i

        case f@ForwardSensorList(target, entity) =>
            val tdl = "[" + fileDetails.map(t => s"{ ${t.tag.toJson} }").mkString(",") + "]"
            log.info(tdl)
            f.forward(tdl)

    }

    def start() = {
        log.info("run started")

        expected = fileDetails.length
        received = 0
        lastTimestamp = 0L

        fileDetails.foreach { fi =>
            val reader = context.actorOf(Reader.props(fi, self))
            context.watch(reader)
            reader ! NextLine
        }
        self ! Continue
    }

    def publish(message: Message) = {
        // log.info(message.toString)
        KafkaBroker.send(topic, message.toJson)
    }

    def hold(timestamp: Long) = {
        // first time or as-fast-as-possible
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


    val fileDetails: List[FileInfo] = {
        fileList flatMap { fname => fileInfo(fname) }
    }

    def fileInfo(fname: String): Option[FileInfo] = {
        MetaDataReader.tagsMap get toTag(fname) match {
            case Some(tagInfo) =>
                Some(FileInfo(fname, tagInfo))
            case None =>
                log error s"Unable to find tag information for file $fname. Ignoring."
                None
        }
    }

    def fileList = {
        val dir = ConfigFactory.load().getString("data.folder")
        val d = new File(dir)
        d.listFiles.filter(_.isFile).map(_.getCanonicalPath).toList
    }

    def toTag(fname: String) = {
        fname.replaceAll(".*/", "").replaceAll("\\.csv", "")
    }

}

object Clock {
    def props() = Props(new Clock())
    case object Continue
    case object Start
    case class AdjustSpeed(speed: Int)
}

