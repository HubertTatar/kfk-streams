package io.huta.common

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig
import scala.jdk.CollectionConverters._
import java.util.Properties

trait AdminConnectionProps {

  def kfkProps(): Properties = {
    val props = new Properties
    props.putAll(
      Map(
        StreamsConfig.APPLICATION_ID_CONFIG -> "admin",
        StreamsConfig.BOOTSTRAP_SERVERS_CONFIG -> "127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094",
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
        StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG -> Serdes.String().getClass,
        StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG -> Serdes.String().getClass
      ).asJava
    )
    props
  }

}
