package io.huta.joins.dsl

import io.huta.common._
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{IntegerSerializer, Serde, Serdes}
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.kstream.JoinWindows
import org.apache.kafka.streams.scala.StreamsBuilder

import java.time.Duration
import java.util.Properties
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters._

object StreamToStreamInner extends AdminConnectionProps with ProducerDefault with Logging {

  def main(args: Array[String]): Unit = {
//    setup(kfkProps())
//    new Thread(producer1(producerProperties())).start()
//    new Thread(producer2(producerProperties())).start()
    join()
  }

  def join(): Unit = {
    //implcits for Consumed.with for builder.stream
    import org.apache.kafka.streams.scala.ImplicitConversions._
    import org.apache.kafka.streams.scala.serialization.Serdes._
    implicit def stream1Serde: Serde[Stream1Data] = Serdes.serdeFrom(new JsonSerializer[Stream1Data], new JsonDeserializer[Stream1Data])
    implicit def stream2Serde: Serde[Stream2Data] = Serdes.serdeFrom(new JsonSerializer[Stream2Data], new JsonDeserializer[Stream2Data])
    implicit def streamJoined: Serde[StreamJoined] = Serdes.serdeFrom(new JsonSerializer[StreamJoined], new JsonDeserializer[StreamJoined])

    val props = kfkProps()
    val builder = new StreamsBuilder
    val stream1 = builder.stream[Int, Stream1Data]("join_topic_3_part")
    val stream2 = builder.stream[Int, Stream2Data]("join_topic_6_part")

    val streamsJoined = stream1.join(stream2)(
      (s1data, s2data) => StreamJoined(s1data.key, s2data.key, s1data.status, s2data.status),
      JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(30))
    ).to("join_output_diff_part")

    val streams = new KafkaStreams(builder.build(), props)
    streams.cleanUp()
    streams.start()

    sys.ShutdownHookThread {
      streams.close(Duration.ofSeconds(5))
    }
  }

  def producer2(props: Properties): Runnable = {
    () => {
      val producer = new KafkaProducer(props, new IntegerSerializer, new JsonSerializer[Stream2Data])

      for (i <- 1 to 1_000_000 by 2) {
        producer.send(new ProducerRecord("join_topic_6_part", i, Stream2Data(i, "StatusA")))
      }
      log.info("producer 2 finished")
      producer.flush()
      producer.close()
    }
  }

  def producer1(props: Properties): Runnable = {
    () => {
      val producer = new KafkaProducer(props, new IntegerSerializer, new JsonSerializer[Stream1Data])

      for (i <- 1 to 1_000_000 by 3) {
        producer.send(new ProducerRecord("join_topic_3_part", i, Stream1Data(i, "StatusB")))
      }
      log.info("producer 1 finished")
      producer.flush()
      producer.close()
    }
  }

  def setup(props: Properties): Unit = {
    def admin = AdminClient.create(props)
    val topic1 = new NewTopic("join_topic_3_part", 3, 3.toShort)
    val topic2 = new NewTopic("join_topic_6_part", 3, 3.toShort)
    val topic3 = new NewTopic("join_output_diff_part", 3, 3.toShort)
    val result = admin.createTopics(List(topic1, topic2, topic3).asJava)
    result.all().get(10, TimeUnit.SECONDS)
    admin.close()
  }
}
