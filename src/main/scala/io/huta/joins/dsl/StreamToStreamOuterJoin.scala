package io.huta.joins.dsl

import io.huta.common.{AdminConnectionProps, JsonDeserializer, JsonSerializer, Logging, ProducerDefault, SetupTopic}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{IntegerSerializer, Serde, Serdes, StringDeserializer, StringSerializer}
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.kstream.JoinWindows
import org.apache.kafka.streams.scala.StreamsBuilder

import java.time.Duration
import java.util.Properties
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters._

object StreamToStreamOuterJoin extends AdminConnectionProps with ProducerDefault with SetupTopic with Logging {

  def main(args: Array[String]): Unit = {
    //    setup(kfkProps())
    //    producer2(producerProperties())
    //    producer1(producerProperties())
    //    consumer()
    join()
  }

  def join(): Unit = {
    // implcits for Consumed.with for builder.stream
    import org.apache.kafka.streams.scala.ImplicitConversions._
    import org.apache.kafka.streams.scala.serialization.Serdes._
    implicit def stream1Serde: Serde[Stream1Data] =
      Serdes.serdeFrom(new JsonSerializer[Stream1Data], new JsonDeserializer[Stream1Data])

    implicit def stream2Serde: Serde[Stream2Data] =
      Serdes.serdeFrom(new JsonSerializer[Stream2Data], new JsonDeserializer[Stream2Data])

    implicit def streamJoined: Serde[StreamJoined] =
      Serdes.serdeFrom(new JsonSerializer[StreamJoined], new JsonDeserializer[StreamJoined])

    val props = kfkProps()
    val builder = new StreamsBuilder
    val stream1 = builder.stream[Int, Stream1Data]("join_topic_1")
    val stream2 = builder.stream[Int, Stream2Data]("join_topic_2")

    val streamsJoined = stream1
      .outerJoin(stream2)(
        (s1data, s2data) => StreamJoined(s1data.key, s2data.key, s1data.status, s2data.status),
        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(20))
      )
      .to("join_output")

    val streams = new KafkaStreams(builder.build(), props)
    streams.cleanUp()
    streams.start()

    sys.ShutdownHookThread {
      streams.close(Duration.ofSeconds(5))
    }
  }

  def producer2(props: Properties): Unit = {
    val producer = new KafkaProducer(props, new IntegerSerializer, new JsonSerializer[Stream2Data])

    for (i <- 1 to 1_000_000) {
      producer.send(new ProducerRecord("join_topic_2", i, Stream2Data(i, "StatusA")))
    }
    log.info("producer 2 finished")
    producer.flush()
    producer.close()
  }

  def producer1(props: Properties): Unit = {
    val producer = new KafkaProducer(props, new IntegerSerializer, new JsonSerializer[Stream1Data])

    for (i <- 1 to 1_000_000) {
      producer.send(new ProducerRecord("join_topic_1", i, Stream1Data(i, "StatusB")))
    }
    log.info("producer 1 finished")
    producer.flush()
    producer.close()
  }

  def consumer(): Unit = {
    val consumer = new KafkaConsumer(consumerProperties(), new StringDeserializer, new JsonDeserializer[Stream2Data])

    consumer.subscribe(List("join_topic_2").asJava)

    var i = 0

    while (i < 5) {
      log.info("poll")
      val records = consumer.poll(Duration.ofSeconds(10))
      records.forEach { record =>
        log.info(s"${record.value()}")
      }
      i += 1
    }
    consumer.close()
  }

  def setup(props: Properties, topics: List[String]): Unit = {
    setupTopics(props, List("join_topic_1", "join_topic_2", "join_output"))
  }

  def consumerProperties(): Properties = {
    val props = new Properties()
    props.putAll(
      Map(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094",
        ConsumerConfig.GROUP_ID_CONFIG -> "test",
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false",
        ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG -> "false"
      ).asJava
    )
    props
  }
}
