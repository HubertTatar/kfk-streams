package io.huta.wordCount

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties
import scala.jdk.CollectionConverters._

object WordCountProducer {

  def main(args: Array[String]): Unit = {
    val props = producerProperties()
    val producer = new KafkaProducer(props, new StringSerializer, new StringSerializer)

    for (i <- 1 to 100) {
      producer.send(new ProducerRecord("words_to_count", ("some fancy text about love")))
    }
    producer.flush()
    producer.close
  }

  def producerProperties(): Properties = {
    val props = new Properties()
    props.putAll(
      Map(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> "127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094",
        ProducerConfig.ACKS_CONFIG -> "all",
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer",
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer"
      ).asJava
    )
    props
  }
}
