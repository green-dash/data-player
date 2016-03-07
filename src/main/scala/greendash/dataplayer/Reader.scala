package greendash.dataplayer

import akka.actor.{Props, Actor, ActorRef}
import org.joda.time.format.DateTimeFormat

import scala.io.Source

class Reader(fname: String, clock: ActorRef) extends Actor {
    import Reader._

    val bufferedSource = Source.fromFile(fname)
    val lines = bufferedSource.getLines()
    val topic = topicName(fname)

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

            Message(topic, ts, value)
        }
        else {
            bufferedSource.close()
            context.stop(self)
            // need to return something to make the compiler happy
            new EmptyMessage
        }
    }

    def topicName(s: String) = s.replaceAll(".*/", "").replaceFirst("\\.csv$", "").replaceAll("\\W", "-")

    override def receive = {
        case NextLine => clock ! nextLine()
    }

}

object Reader {
    def props(fname: String, clock: ActorRef) = Props(new Reader(fname, clock))
    class EmptyMessage()
    case class Message(topic: String,
                       timestamp: Long,
                       value: Double) extends EmptyMessage
    case object NextLine
}

