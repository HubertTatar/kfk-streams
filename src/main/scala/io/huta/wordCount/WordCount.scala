package io.huta.wordCount

import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.scala.StreamsBuilder

import java.time.Duration
import java.util.Properties
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters._

object WordCount extends Logging {

  def main(args: Array[String]): Unit = {
//    setup(kfkProps())
    stream()
  }

    def stream(): Unit = {
    import org.apache.kafka.streams.scala.ImplicitConversions._
    import org.apache.kafka.streams.scala.serialization.Serdes._

    val props = kfkProps()
    val builder = new StreamsBuilder

    builder.stream[String, String]("words_to_count")
      .mapValues(_.toLowerCase())
      .flatMapValues(txt => txt.split(" "))
      .groupBy((_, word) => word)
      .count()
      .toStream
      .to("words_counted")

    val streams = new KafkaStreams(builder.build(), props)
    streams.cleanUp()
    streams.start()

    sys.ShutdownHookThread {
      streams.close(Duration.ofSeconds(5))
    }
  }

  def setup(props: Properties): Unit = {
    def admin = AdminClient.create(props)
    val topic1 = new NewTopic("words_to_count", 3, 3.toShort)
    val topic2 = new NewTopic("words_counted", 3, 3.toShort)
    val result = admin.createTopics(List(topic1, topic2).asJava)
    result.all().get(10, TimeUnit.SECONDS)
  }

  def kfkProps(): Properties = {
    val props = new Properties
     props.putAll(
      Map(
        StreamsConfig.APPLICATION_ID_CONFIG -> "starter",
        StreamsConfig.BOOTSTRAP_SERVERS_CONFIG -> "127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094",
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
        StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG -> Serdes.String().getClass,
        StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG -> Serdes.String().getClass,
      ).asJava
    )
    props
  }
}
