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

    def nextLine() = {
        if (lines.hasNext) {
            val line = lines.next()
            val Array(dt, v) = line.split(",")

            // todo: send no message when value is empty
            val vw = v.replaceAll("\"", "")
            val value = if (vw.isEmpty) Double.NaN else vw.toDouble

            val dateTime = fmt.parseDateTime(dt.replaceAll("\"", ""))
            val ts = dateTime.getMillis

            Message(fileInfo.tag, ts, value)
        }
        else {
            bufferedSource.close()
            context.stop(self)
            // need to return something to make the compiler happy
            new EmptyMessage
        }
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

