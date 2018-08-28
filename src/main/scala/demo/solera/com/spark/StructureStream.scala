package demo.solera.com.spark

import java.util.concurrent.TimeUnit

import demo.solera.com.conf.Conf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.ConstantInputDStream


object StructureStream {

  def main(args: Array[String]): Unit = {

    val ssc = new StreamingContext(Conf.sparkConf, Seconds(1))
    val sparkSession = SparkSession.builder().
      config(Conf.sparkConf.setAppName("Spark-Kafka")).
      getOrCreate()

    import sparkSession.implicits._

    val csvPath = Conf.sparkConf.get("csv.path")
    val csvSchema = StructType(
      StructField("districtId", IntegerType, true) ::
        StructField("csv_description", StringType, true) ::
        StructField("total", LongType, true) :: Nil
    )

    /*var csvDFT = sparkSession.read.
      schema(csvSchema).csv("/tmp/spark-test")*/

    def getData = sparkSession.read.format("csv")
      .option("header", "true")
      .schema(csvSchema)
      .load(csvPath)
      .toDF

    val bashInterval = Conf.sparkConf.getInt("bash.interval", 10)
    val refreshCSV = new  ConstantInputDStream(ssc, ssc.sparkContext.parallelize(Seq()))
      .window(Seconds(bashInterval), Seconds(bashInterval))

    var csvDF = getData
    csvDF.cache
    refreshCSV.foreachRDD{_ =>
      csvDF.unpersist(true)
      csvDF = getData
      csvDF.cache
      csvDF.show
    }

    val streamSchema = StructType(
      StructField("transactionID", LongType) ::
        StructField("description", StringType) ::
        StructField("timeStamp", TimestampType) ::
        StructField("districtId", IntegerType) :: Nil
    )

    val streamDF = sparkSession
      .readStream
      .format("kafka")
      .option("subscribe", Conf.topic)
      .option("kafka.bootstrap.servers", "localhost:9092")
      .load().selectExpr("CAST(value AS STRING)").as[String]
      .select(from_json($"value", streamSchema).as("data"))
      .select("data.*")


    val streamTrigger = Trigger.ProcessingTime(
      Conf.sparkConf.getInt("stream.interval",2), TimeUnit.SECONDS
    )

    val query1 = streamDF
      .withColumn("date_year",year(col("timeStamp")))
      .withColumn("date_month",month(col("timeStamp")))
      .withColumn("date_day",dayofmonth(col("timeStamp")))
      .withColumn("date_hour",hour(col("timeStamp")))
      .join(csvDF,Seq("districtId"),"leftOuter")
      .writeStream
      .trigger(streamTrigger)
      /*.format("console")
      .option("truncate", "false")
      .start*/
      .format("parquet")
      .option("path","hdfs://127.0.0.1:8020/output/")
      .partitionBy("date_year", "date_month", "date_day", "date_hour")
      .start

    ssc.start
    query1.awaitTermination
    ssc.awaitTermination
  }
}
