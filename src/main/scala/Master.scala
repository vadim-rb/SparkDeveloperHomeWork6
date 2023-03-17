import com.github.plokhotnyuk.jsoniter_scala.core.{JsonValueCodec, readFromString, writeToArray, writeToString}
import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker
import kantan.csv.ops.toCsvInputOps
import kantan.csv._
import kantan.csv.ops._
import kantan.csv.generic._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.Properties
import scala.io.Codec.fallbackSystemCodec
import scala.util.{Failure, Success, Try, Using}
import scala.io.{BufferedSource, Source}
import scala.util.Random
import org.apache.kafka.common.serialization.{IntegerSerializer, StringSerializer}
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{IntegerDeserializer, IntegerSerializer, StringDeserializer}
import scala.jdk.CollectionConverters._
import java.util.Properties
import java.time.Duration
import java.util

object Master {

  private val bootstrapServer = "localhost:29092"

  private def consume15(numberOfMessagesToRead: Int, topic: String, partition: Int): List[ConsumerRecord[Integer, String]] = {
    val props = new Properties()
    props.put("bootstrap.servers", bootstrapServer)
    props.put("group.id", "consumer1")
    props.put("auto.offset.reset", "earliest")
    props.put("enable.auto.commit", "false")
    props.put("max.poll.records", s"$numberOfMessagesToRead")
    val consumer = new KafkaConsumer(props, new IntegerDeserializer, new StringDeserializer)
    val partitionToReadFrom = new TopicPartition(topic, partition)
    consumer.assign(util.Arrays.asList(partitionToReadFrom))
    var offsetToReadFrom = 0L
    var nowOffset = 0L
    consumer.seekToEnd(util.Arrays.asList(partitionToReadFrom))
    val lastOffset = consumer.position(partitionToReadFrom)
    var nowBuffer: Iterable[ConsumerRecord[Integer, String]] = None
    var lastBuffer: Iterable[ConsumerRecord[Integer, String]] = None
    while (nowOffset < lastOffset) {
      lastBuffer = nowBuffer
      consumer.seek(partitionToReadFrom, offsetToReadFrom)
      nowBuffer = consumer.poll(Duration.ofSeconds(1)).asScala
      nowBuffer
        .foreach {
          msg =>
            nowOffset = msg.offset + 1
        }
      offsetToReadFrom += numberOfMessagesToRead
    }
    consumer.close()
    List(lastBuffer, nowBuffer).flatten
  }

  def main(args: Array[String]): Unit = {
    val topic = "books"
    val numberOfMessagesToRead = 15
    val partitions = Seq(0,1,2)
    val random = new Random
    implicit val codecOut: JsonValueCodec[Book] = JsonCodecMaker.make
    val props = new Properties()
    props.put("bootstrap.servers", bootstrapServer)
    props.put("acks","all")
    val producer = new KafkaProducer(props, new IntegerSerializer, new StringSerializer)
    val rawData: java.net.URL = getClass.getResource("/bestsellers_with_categories.csv")
    val reader: CsvReader[ReadResult[Book]] = rawData.asCsvReader[Book](rfc.withHeader)
    println("Produce ...")
    try {
      reader.foreach {
        case Left(s) => println(s"Error: $s")
        case Right(i) =>
          val json = writeToString(i);
          println(json);
          producer.send(new ProducerRecord(topic, partitions(random.nextInt(partitions.length)), json))
      }
    } finally {
      producer.flush()
      producer.close()
    }
    println("Consume ...")
    for (p<-partitions){
      val buf: List[ConsumerRecord[Integer, String]] = consume15(numberOfMessagesToRead, topic, p)
      buf.reverse.take(5).foreach {
        msg =>
          println(s"${msg.partition}\t${msg.offset}\t${msg.key}\t${msg.value}");
      }
      println("-------------------------------")
    }

  }
}