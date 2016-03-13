package greendash.dataplayer

import akka.actor.{Actor, ActorRef, Props}
import com.typesafe.config.ConfigFactory
import greendash.dataplayer.Clock.AdjustSpeed
import spray.can.Http
import spray.http.HttpMethods._
import spray.http.{HttpRequest, Uri, _}


class RestService(clock: ActorRef) extends Actor {

    val config = ConfigFactory.load()
    var speedFactor = config.getInt("speed.factor")

    override def receive: Receive = {

        // when a new connection comes in we register ourselves as the connection handler
        case _: Http.Connected => sender ! Http.Register(self)

        case HttpRequest(POST, Uri.Path("/speed"), _, entity: HttpEntity.NonEmpty, _) =>
            try {
                speedFactor = entity.asString.toInt
                sender ! HttpResponse(entity = "OK")
                clock ! AdjustSpeed(speedFactor)
            } catch {
                case e: Throwable =>
                    sender ! HttpResponse(entity = "NOK: " + e.getMessage)
            }

        case HttpRequest(GET, Uri.Path("/speed"), _, entity, _) =>
            sender ! HttpResponse(entity = speedFactor.toString)

        case HttpRequest(GET, Uri.Path("/sensorList"), _, entity: HttpEntity, _) =>
            val target = sender
            clock ! ForwardSensorList(target, entity)

        case HttpRequest(GET, Uri.Path("/state"), _, entity: HttpEntity, _) =>
            val target = sender
            clock ! ForwardState(target, entity)

    }
}

object RestService {
    def props(clock: ActorRef) = Props(new RestService(clock))
}

class ForwardHttpResponse(target: ActorRef, entity: HttpEntity) {
    def forward(answer: String) = target ! HttpResponse(entity = answer)
}

case class ForwardSensorList(target: ActorRef, entity: HttpEntity) extends ForwardHttpResponse(target, entity)
case class ForwardState(target: ActorRef, entity: HttpEntity) extends ForwardHttpResponse(target, entity)
