package io.huta.wordCount

import io.huta.common.Logging
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}

import java.time.Duration
import java.util.Properties
import scala.jdk.CollectionConverters._

object WordCountConsumer extends Logging {

  def main(args: Array[String]): Unit = {
    val consumer = new KafkaConsumer(consumerProperties())

    consumer.subscribe(List("words_counted").asJava)

    var i = 0

    while(i < 5) {
      log.info("poll")
      val records = consumer.poll(Duration.ofSeconds(10))
      records.forEach { record =>
        log.info(s"${record.value()}")
      }
      i += 1
    }
    consumer.close()
  }

  def consumerProperties(): Properties = {
    val props = new Properties()
    props.putAll(Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094",
      ConsumerConfig.GROUP_ID_CONFIG -> "word_count2",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",
      ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG -> "false",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.LongDeserializer"

    ).asJava)
    props
  }
}
