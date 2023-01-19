package io.huta.split

import io.huta.common.{AdminConnectionProps, JsonSerializer, Logging, ProducerDefault, SetupTopic}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.scala.StreamsBuilder

import java.time.Duration
import java.util.Properties

object OneTopicToManyTopics extends AdminConnectionProps with ProducerDefault with SetupTopic with Logging {

  def main(args: Array[String]): Unit = {
//        setup(kfkProps())
    new Thread(producer(producerProperties())).start()
    oneToMany()
  }

  def setup(props: Properties) = {
    setupTopics(props, List("onetomany_original", "onetomany_1", "onetomany_2", "onetomany_def"))
  }

  def oneToMany(): Unit = {
    // implcits for Consumed.with for builder.stream
    import org.apache.kafka.streams.scala.ImplicitConversions._
    import org.apache.kafka.streams.scala.serialization.Serdes._

    val props = kfkProps()
    val builder = new StreamsBuilder

    val orgStream = builder.stream[Int, String]("onetomany_original")

    orgStream
      .filter { (k, _) => k % 2 != 0 }
      .to("onetomany_1")

    orgStream
      .filter { (k, _) => k % 2 == 0 }
      .to("onetomany_2")

    orgStream
      .to("onetomany_def")

    val streams = new KafkaStreams(builder.build(), props)
    streams.cleanUp()
    streams.start()

    sys.ShutdownHookThread {
      streams.close(Duration.ofSeconds(5))
    }
  }

  def producer(props: Properties): Runnable =
    () => {
      val producer = new KafkaProducer(props, new IntegerSerializer, new JsonSerializer[SplitData])
      for (i <- 0 to 1_000_000) {
        producer.send(new ProducerRecord("onetomany_original", i, SplitData(i, s"some_data$i")))
      }
      producer.flush()
      producer.close()
      log.info("producer stopped")
    }

  case class SplitData(key: Int, data: String)
}
