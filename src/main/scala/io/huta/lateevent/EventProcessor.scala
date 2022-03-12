package io.huta.lateevent

import io.huta.common.Logging
import io.huta.joins.dsl.Stream1Data
import io.huta.joins.dsl.StreamToStreamOuterJoin.kfkProps
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsConfig}
import org.apache.kafka.streams.kstream.TimeWindows
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.Consumed

import java.time.Duration

//./kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094 --topic aggs --from-beginning --formatter kafka.tools.DefaultMessageFormatter --property print.key=true --property print.value=true --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
object EventProcessor extends Logging {

  def main(args: Array[String]): Unit = {
    val props = kfkProps()
    val builder = new StreamsBuilder
    //implcits for Consumed.with for builder.stream
    import org.apache.kafka.streams.scala.ImplicitConversions._
    import org.apache.kafka.streams.scala.serialization.Serdes._
    builder
      .stream[String, String]("late_event")
      .groupBy((k,v) => "dummy")
      .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(10)))
//      .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofSeconds(10), Duration.ofSeconds(10)))
      .count()
      .toStream
      .map{ (ws, i) => (s"${ws.window().start()}", s"$i") }
      .to("aggs")

    val topology = builder.build()

    //update every time an event is received
    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "0")

    val streams = new KafkaStreams(topology, props)
    streams.cleanUp()
    streams.start()

    sys.ShutdownHookThread {
      streams.close(Duration.ofSeconds(5))
    }

  }
}
