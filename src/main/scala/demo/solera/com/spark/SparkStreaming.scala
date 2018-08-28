package demo.solera.com.spark

import demo.solera.com.conf.Conf
import demo.solera.com.model.Event
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.streaming.dstream.ConstantInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferBrokers
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.read
import org.apache.spark.sql.functions._


object SparkStreaming {

  def main(args: Array[String]): Unit = {

    val sparkContext = new SparkContext(Conf.sparkConf.setAppName("Spark-Kafka"))
    val ssc = new StreamingContext(sparkContext, Seconds(1))
    val spark = SparkSession.builder.config(sparkContext.getConf).getOrCreate
    import spark.implicits._

    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc, PreferBrokers, Subscribe[String, String](Array(Conf.topic), Conf.kafkaParams))
      .map(_.value)

    val csvSchema = StructType(
      StructField("districtId", IntegerType) ::
        StructField("csv_description", StringType) ::
        StructField("total", LongType) :: Nil
    )

    val csvPath = Conf.sparkConf.get("csv.path")
    def getData = spark.read.format("csv")
      .option("header", "true")
      .schema(csvSchema)
      .load(csvPath)
      .toDF

    val bashInterval = Conf.sparkConf.getInt("bash.interval", 10)
    val refreshCSV = new  ConstantInputDStream(ssc, sparkContext.parallelize(Seq()))
      .window(Seconds(bashInterval), Seconds(bashInterval))

    var csv = getData
    csv.cache
    refreshCSV.foreachRDD{_ =>
      csv.unpersist(true)
      csv = getData
      csv.cache
    }

    val windowsTime = Conf.sparkConf.getInt("stream.interval", 2)
    kafkaStream.window(Seconds(windowsTime)).foreachRDD { rdd =>

      val eventsDataFrame = rdd.map(value => {
        implicit val formats = DefaultFormats
        read[Event](value)
      }).toDF

      eventsDataFrame
        .join(csv,Seq("districtId"),"left_outer")
        .select('*, year(to_timestamp($"timeStamp")) as 'date_year, month(to_timestamp($"timeStamp")) as 'date_month,
          dayofmonth(to_timestamp($"timeStamp")) as 'date_day, hour(to_timestamp($"timeStamp")) as 'date_hour)
        //.show()
        .write.partitionBy("date_year","date_month", "date_day", "date_hour")
        .mode(SaveMode.Overwrite)
        .parquet("hdfs://localhost:8020/output/")
    }

    ssc.start
    ssc.awaitTermination
  }
}
