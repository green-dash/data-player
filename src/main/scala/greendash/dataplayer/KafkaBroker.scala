package greendash.dataplayer

import java.util.Properties

import com.typesafe.config.{ConfigValue, ConfigFactory}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.collection.JavaConversions._

object KafkaBroker {

    val props = new Properties()

    val config = ConfigFactory.load().getConfig("kafkaBroker")

    val es = config.entrySet.toSet
    es.foreach { s =>
        val k: String = s.getKey
        val v = s.getValue.unwrapped().asInstanceOf[String]
        props.put(k, v)
    }

    val producer = new KafkaProducer[String, String](props)

    def send(topic: String, message: String) = producer.send(new ProducerRecord[String, String](topic, message))

}
