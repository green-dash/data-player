package greendash.dataplayer

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import greendash.dataplayer.model.{FileInfo, TagDetails}
import org.joda.time.format.DateTimeFormat

import scala.io.Source

class Reader(fileInfo: FileInfo, clock: ActorRef) extends Actor with ActorLogging {
    import Reader._

    val bufferedSource = Source.fromFile(fileInfo.fileName)
    val lines = bufferedSource.getLines()

    val fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")

    clock ! nextLine()

    def nextLine(): EmptyMessage = {
        if (lines.hasNext) {
            val line = lines.next()
            parseLine(line)
        }
        else {
            endOfFile()
        }
    }

    def parseLine(line: String) = {
        val Array(dt, v) = line.split(",").map(_.trim.replaceAll("\"", ""))
        if (v.isEmpty) {
            new EmptyMessage
        } else {
            val value = v.toDouble
            val dateTime = fmt.parseDateTime(dt)
            val ts = dateTime.getMillis
            Message(fileInfo.tag, ts, value)
        }
    }

    def endOfFile() = {
        log.info(s"$fileInfo: reader finished")
        bufferedSource.close()
        context.stop(self)
        new EmptyMessage // need to return something to make the compiler happy
    }

    override def receive = {
        case NextLine => clock ! nextLine()
    }

}

object Reader {
    def props(fileInfo: FileInfo, clock: ActorRef) = Props(new Reader(fileInfo, clock))
    class EmptyMessage()
    case class Message(tagDetails: TagDetails,
                       timestamp: Long,
                       value: Double) extends EmptyMessage {
        def toJson = s"""{ ${tagDetails.toJson}, "timestamp": $timestamp, "value": $value }"""
    }
    case object NextLine
}

