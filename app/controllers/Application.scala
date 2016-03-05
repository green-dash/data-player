package controllers

import java.io.File
import java.util.{Calendar, Properties}
import javax.inject.{Inject, Singleton}

import akka.actor._
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.joda.time.format.DateTimeFormat
import play.api.Play._
import play.api.mvc._

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.io.Source



@Singleton
class Application @Inject()(val system: ActorSystem) extends Controller with LazyLogging {
    val dir = current.configuration.getString("fileFolder").get
    val flist = getListOfFiles(dir)

    val clock = system.actorOf(Clock.props(flist))

    clock ! Continue

    def index = Action {
        Ok(views.html.index("Your new application is ready."))
    }

    def getListOfFiles(dir: String):List[String] = {
        val d = new File(dir)
        d.listFiles.filter(_.isFile).map(_.getCanonicalPath).toList
    }

}

class Clock(files: List[String]) extends Actor with ActorLogging {

    val messageMap = mutable.Map[ActorRef, Message]()

    val runStart = Calendar.getInstance.getTimeInMillis

    log.info("run started")

    var expected = files.length
    var received = 0
    var lastTimestamp = 0L
    val speedFactor = current.configuration.getInt("speedFactor").get

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
                context.stop(self)
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
            val duration = delta / speedFactor
            // log.info(s"duration: $duration")
            val sleep = Duration(duration, MILLISECONDS)
            lastTimestamp = timestamp
            scheduler.scheduleOnce(sleep, self, Continue)
        }
    }
}

object Clock {
    def props(files: List[String]) = Props(new Clock(files))
}

class EmptyMessage()
case class Message(topic: String, timestamp: Long, value: Double) extends EmptyMessage
case object Continue
case object NextLine

class Reader(fname: String, clock: ActorRef) extends Actor {
    val bufferedSource = Source.fromFile(fname)
    val lines = bufferedSource.getLines()
    val topic = topicName(fname)

    val fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")

    clock ! nextLine()

    def nextLine() = {
        if (lines.hasNext) {
            val line = lines.next()
            val Array(dt, v) = line.split(",")
            val dateTime = fmt.parseDateTime(dt.replaceAll("\"", ""))
            val ts = dateTime.getMillis

            val vw = v.replaceAll("\"", "")
            val value = if (vw.isEmpty) Double.NaN else vw.toDouble

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
}

object KafkaBroker {

    private val config = current.configuration.getConfig("kafkaBroker").get
    val props = new Properties()
    config.keys.foreach { k =>
        props.put(k, config.getString(k).get)
    }
    val producer = new KafkaProducer[String, String](props)

    def send(topic: String, message: String) = producer.send(new ProducerRecord[String, String](topic, message))

}

