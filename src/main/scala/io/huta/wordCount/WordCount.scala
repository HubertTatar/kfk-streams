package io.huta.wordCount

import io.huta.common.{AdminConnectionProps, Logging}
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.scala.StreamsBuilder

import java.time.Duration
import java.util.Properties
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters._

object WordCount extends AdminConnectionProps with Logging {

  def main(args: Array[String]): Unit = {
//    setup(kfkProps())
    stream()
  }

    def stream(): Unit = {
      //implcits for Consumed.with for builder.stream
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
}
