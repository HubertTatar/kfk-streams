package io.huta

import java.time.{Duration, Instant, LocalDateTime, ZoneId, ZonedDateTime}
import java.util.{Properties, TimeZone}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition

import collection.JavaConverters._

object OffsetsForTime extends App {

  implicit def toJavaOffsetQuery(
      offsetQuery: Map[TopicPartition, scala.Long]
  ): java.util.Map[TopicPartition, java.lang.Long] =
    offsetQuery.map { case (tp, time) => tp -> new java.lang.Long(time) }.asJava

  val topic = "adx_results_processed_dk"
  import java.sql.Timestamp
  val time = LocalDateTime.of(2022, 10, 7, 6, 25)
  val utcDateTime = ZonedDateTime.of(time, ZoneId.of("UTC"))
  val timestampDate = Timestamp.valueOf(utcDateTime.toLocalDateTime());
  val timestamp = timestampDate.getTime

  val props = new Properties()
  props.put("group.id", "test-consumer-group")
  props.put("bootstrap.servers", "cluster")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("auto.offset.reset", "earliest")
  val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)

  val partitions = (0 to 10).map(num => new TopicPartition(topic, num))

  consumer.assign(partitions.asJava)
  // dummy poll before calling seek
  consumer.poll(Duration.ofMillis(500))

  val partitionMap = partitions.map(part => (part, timestamp)).toMap

  //  val partitionMap = partitions.foldLeft(Map[TopicPartition, Long]) { (map, str) => map += (str, timestamp)  }

  // get next available offset after given timestamp
  val topicPartOffset = consumer.offsetsForTimes(partitionMap).asScala // .head

  //  topicPartOffset.foreach(tpo => consumer.seek(tpo._1, tpo._2.offset()))
  // seek to offset
  //  consumer.seek(topicPartition, offsetAndTimestamp.offset())
  //
  topicPartOffset.foreach(tpo => {
    val offsetAndTimestamp = tpo._2
    val date = LocalDateTime.ofInstant(
      Instant.ofEpochMilli(offsetAndTimestamp.timestamp()),
      TimeZone
        .getDefault()
        .toZoneId()
    );

    val zoned = ZonedDateTime.ofInstant(Instant.ofEpochMilli(offsetAndTimestamp.timestamp()), ZoneId.of("UTC"))

    println(
      s"Offset: ${offsetAndTimestamp.offset()}, timestamp: ${offsetAndTimestamp.timestamp()}: date: ${date}, zoned: ${zoned}"
    )
  })

  // poll data
  //  val record = consumer.poll(Duration.ofMillis(10000)).asScala.toList
  //
  //  for (data <- record) {
  //    println(s"Timestamp: ${data.timestamp()}, Key: ${data.key()}, Value: ${data.value()}")
  //  }

}
