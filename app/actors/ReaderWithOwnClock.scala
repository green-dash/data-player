package actors

import akka.actor.{Actor, Props}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.io.Source

class ReaderWithOwnClock(fname: String) extends Actor {

    val bufferedSource = Source.fromFile(fname)
    val lines = bufferedSource.getLines()

    var (ts, msg) = nextLine()

    def nextLine() = {
        if (lines.hasNext) {
            val line = lines.next()
            val Array(ts, msg) = line.split(",")
            (ts.toLong, msg)
        }
        else {
            bufferedSource.close()
            context.stop(self)
            (0L, "") // need to return something to make the compiler happy
        }
    }

    // nice try, but the akka scheduler is not accurate enough to synchronize independent actors
    def readWithOwnClock = {
        print(s"$fname: $ts $msg\n")
        val (nextTs: Long, nextMsg: String) = nextLine()
        val wait = Duration(nextTs - ts, MILLISECONDS)
        ts = nextTs
        msg = nextMsg
        context.system.scheduler.scheduleOnce(wait, self, "read")
    }

    override def receive: Receive = {
        case "read" => readWithOwnClock
    }
}

object ReaderWithOwnClock {
    def props(fname: String) = Props(new ReaderWithOwnClock(fname))
}
