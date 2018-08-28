package demo.solera.com.kafka

import demo.solera.com.conf.Conf
import demo.solera.com.model.Event
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.util.Random
import org.codehaus.jackson.map.ObjectMapper
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write

object Producer extends App {

  implicit val formats = DefaultFormats
  val rnd = new Random()

  val producer = new KafkaProducer[String, String](mapAsJavaMap(Conf.kafkaParams))
  val t = System.currentTimeMillis()
  val mapper = new ObjectMapper()

  @tailrec
  def infiniteLoop(nEvents:Long):Unit={
    val msg = write(Event(nEvents,"description",1493462850L,7989))
    println(msg)
    val data = new ProducerRecord[String, String](Conf.topic, nEvents.toString, msg)
    producer.send(data)
    Thread.sleep(1)
    infiniteLoop(nEvents+1L)
  }
  infiniteLoop(1)
}