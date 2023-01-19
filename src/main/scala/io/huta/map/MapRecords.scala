package io.huta.map

import io.huta.common.{AdminConnectionProps, JsonSerializer, Logging, ProducerDefault, SetupTopic}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.scala.StreamsBuilder

import java.time.Duration
import java.util.Properties

object MapRecords extends AdminConnectionProps with ProducerDefault with SetupTopic with Logging {

  def main(args: Array[String]): Unit = {
//    setup(kfkProps())
    new Thread(producer(producerProperties())).start()
    mapping()
  }

  def mapping(): Unit = {
    import org.apache.kafka.streams.scala.ImplicitConversions._
    import org.apache.kafka.streams.scala.serialization.Serdes._

    val props = kfkProps()
    val builder = new StreamsBuilder

    builder
      .stream[Int, String]("map_original")
      .mapValues(_.toUpperCase())
      .to("map_mapped")

    val streams = new KafkaStreams(builder.build(), props)
    streams.cleanUp()
    streams.start()

    sys.ShutdownHookThread {
      streams.close(Duration.ofSeconds(5))
    }
  }

  def producer(props: Properties): Runnable =
    () => {
      val producer = new KafkaProducer(props, new IntegerSerializer, new JsonSerializer[OrgData])
      for (i <- 0 to 1_000_000) {
        producer.send(new ProducerRecord("map_original", i, OrgData(i, s"some_data$i")))
      }
    }

  def setup(props: Properties) = {
    setupTopics(props, List("map_original", "map_mapped"))
  }

  case class OrgData(id: Int, name: String)
  case class MappedData(id: Int, name: String)
}
