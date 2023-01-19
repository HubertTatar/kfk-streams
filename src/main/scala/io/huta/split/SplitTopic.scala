package io.huta.split

import io.huta.common.{AdminConnectionProps, JsonSerializer, Logging, ProducerDefault, SetupTopic}
import io.huta.joins.dsl.StreamToStreamLeft.kfkProps
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.Branched

import java.time.Duration
import java.util.Properties

object SplitTopic extends AdminConnectionProps with ProducerDefault with SetupTopic with Logging {

  def main(args: Array[String]): Unit = {
//    setup(kfkProps())
    new Thread(producer(producerProperties())).start()
    split()
  }

  def split(): Unit = {
    // implcits for Consumed.with for builder.stream
    import org.apache.kafka.streams.scala.ImplicitConversions._
    import org.apache.kafka.streams.scala.serialization.Serdes._

    val props = kfkProps()
    val builder = new StreamsBuilder

    builder
      .stream[Int, String]("split_original")
      .split()
      .branch((key, _) => key % 2 != 0, Branched.withConsumer[Int, String](ks => ks.to("split_1")))
      .branch((key, _) => key % 2 == 0, Branched.withConsumer[Int, String](ks => ks.to("split_2")))
      .defaultBranch(Branched.withConsumer[Int, String](ks => ks.to("split_def")))

    val streams = new KafkaStreams(builder.build(), props)
    streams.cleanUp()
    streams.start()

    sys.ShutdownHookThread {
      streams.close(Duration.ofSeconds(5))
    }
  }

  def setup(props: Properties) = {
    setupTopics(props, List("split_original", "split_1", "split_2", "split_def"))
  }

  def producer(props: Properties): Runnable =
    () => {
      val producer = new KafkaProducer(props, new IntegerSerializer, new JsonSerializer[SplitData])
      for (i <- 0 to 1_000_000) {
        producer.send(new ProducerRecord("split_original", i, SplitData(i, s"some_data$i")))
      }
      producer.flush()
      producer.close()
      log.info("producer stopped")
    }

  case class SplitData(key: Int, data: String)
}
