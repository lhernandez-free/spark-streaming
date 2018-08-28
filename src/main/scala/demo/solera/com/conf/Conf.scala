package demo.solera.com.conf

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf

object Conf {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  final val topic = "test"
  final val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "zookeeper.connect" -> "localhost:2181",
    "zookeeper.session.timeout.ms" -> "500",
    "zookeeper.sync.timeout.ms" -> "500",
    "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
    "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
    "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
    "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
    "group.id" -> "graphProcessors"
  )
  val sparkConf = new SparkConf()
    .setAppName("Unnamed")
    .setMaster("local[1]")
    .set("bash.interval","10")
    .set("stream.interval","2")
    .set("csv.path","/tmp/spark-test/file")
    .set("spark.sql.streaming.checkpointLocation","/tmp/checkpoint")
    //.set("fs.default.name", "hdfs://localhost:8020")
}
