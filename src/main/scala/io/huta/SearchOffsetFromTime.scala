package io.huta

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer, OffsetAndTimestamp}
import org.apache.kafka.common.TopicPartition
import org.slf4j.{Logger, LoggerFactory}

import java.io.File
import java.sql.Timestamp
import java.time.{Duration, Instant, LocalDateTime, ZoneId, ZonedDateTime}
import java.util.Properties
import java.util.concurrent.Executors
import scala.collection.JavaConverters._
import scala.concurrent.{Await, ExecutionContext, Future}

object SearchOffsetFromTime {

  def main(args: Array[String]): Unit = {
    val partitions = (0 to 20)
      .groupBy(p => p % 4)

    val search = new SearchOffsetFromTime()
    val exCtx = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(4))

    val futures = partitions.mapValues(partitions => read(partitions, search, exCtx))

    Future.firstCompletedOf(futures.values)(exCtx)
  }

  def read(partitions: Seq[Int], search: SearchOffsetFromTime, ex: ExecutionContext) = Future {
    search.read(partitions)
  }(ex)
}
class SearchOffsetFromTime() {

  val log: Logger = LoggerFactory.getLogger(SearchOffsetFromTime.getClass)

  def read(partitions: Seq[Int]) = {
    log.info("Reading partitions: {}", partitions)
    readFromKafka("topic", printEntity, printProto, partitions)
  }

  def printProto(data: Array[Byte]): Unit = {
    ???
  }

  def printEntity(record: Iterable[ConsumerRecord[Array[Byte], Array[Byte]]], printer: Array[Byte] => Unit): Unit = {
    record.iterator
      .foreach { data =>
        if (data.value() != null) {
          printer(data.value())
        } else {
          val key = new String(data.key())
          log.info(s"$key is null")
        }
      }
  }

  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit): Unit = {
    val p = new java.io.PrintWriter(f)
    try {
      op(p)
    } finally {
      p.close()
    }
  }

  def readFromKafka(
      topic: String,
      printFun: (Iterable[ConsumerRecord[Array[Byte], Array[Byte]]], Array[Byte] => Unit) => Unit,
      printer: Array[Byte] => Unit,
      partitionsInt: Seq[Int]
  ): Unit = {
    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](props())

    val partitions = partitionsInt
      .map(num => new TopicPartition(topic, num))

    consumer.assign(partitions.asJava)
    // dummy poll before calling seek
    consumer.poll(Duration.ofMillis(500))

    val timestamp = timestampFor(LocalDateTime.of(2022, 10, 10, 11, 30))
    val partitionMap = partitions.map(part => (part, timestamp)).toMap
    val topicPartOffset: Map[TopicPartition, OffsetAndTimestamp] = consumer.offsetsForTimes(partitionMap).asScala.toMap

    topicPartOffset.foreach(tpo => {
      val topicPartition = tpo._1
      val offsetAndTimestamp = tpo._2
      consumer.seek(topicPartition, offsetAndTimestamp.offset())
      val zoned = ZonedDateTime.ofInstant(Instant.ofEpochMilli(offsetAndTimestamp.timestamp()), ZoneId.of("UTC"))
      log.info(s"Offset: ${offsetAndTimestamp.offset()}, timestamp: ${offsetAndTimestamp.timestamp()}, zoned: ${zoned}")
    })
    var polls = 0L

    while (true) {
      val record: Iterable[ConsumerRecord[Array[Byte], Array[Byte]]] = consumer.poll(Duration.ofMillis(10000)).asScala
      polls += 1
      if (polls % 1000 == 0) {
        if (record.nonEmpty) {
          val first = record.head
          val zoned = ZonedDateTime.ofInstant(Instant.ofEpochMilli(first.timestamp()), ZoneId.of("UTC"))
          log.info(s"Offset: ${first.offset()}, zoned: ${zoned}")
          log.info("{}", record)
        }
        log.info("1000 polls")
      }
      printFun(record, printer)
    }
  }

  implicit def toJavaOffsetQuery(
      offsetQuery: Map[TopicPartition, scala.Long]
  ): java.util.Map[TopicPartition, java.lang.Long] =
    offsetQuery.map { case (tp, time) => tp -> new java.lang.Long(time) }.asJava

  def timestampFor(ldt: LocalDateTime): Long = {
    val utcDateTime = ZonedDateTime.of(ldt, ZoneId.of("UTC"))
    log.info(s"UTC ${utcDateTime} local: ${ldt}")
    val timestampDate = Timestamp.valueOf(utcDateTime.toLocalDateTime());
    timestampDate.getTime
  }

  def props(): Properties = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "cluster")
    props.put(ConsumerConfig.CLIENT_DNS_LOOKUP_CONFIG, "use_all_dns_ips")
    props.put(
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.ByteArrayDeserializer"
    )
    props.put(
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.ByteArrayDeserializer"
    )
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "ht-test-consumer-group")
    props
  }
}
