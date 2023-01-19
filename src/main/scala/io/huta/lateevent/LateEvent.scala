package io.huta.lateevent

import io.huta.common.{AdminConnectionProps, Logging, ProducerDefault}
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.util.{Properties, Timer, TimerTask}
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters._

object LateEvent extends AdminConnectionProps with ProducerDefault with Logging {

  def main(args: Array[String]): Unit = {
//    setup(kfkProps())
    lateEventProducer()
  }

  def lateEventProducer() = {
    val producer = new KafkaProducer[String, String](producerProperties(), new StringSerializer, new StringSerializer)
    val now = System.currentTimeMillis()
    val delay = 1200 - Math.floorMod(now, 1000)
    val timer = new Timer()
    timer.schedule(
      new TimerTask {
        override def run(): Unit = {
          val ts = System.currentTimeMillis()
          val second = Math.floorMod(ts / 1000, 60)

          if (second != 58L) {
            sendMessage(second, ts, "on time", producer)
            producer.flush()
          }
          if (second == 2L) {
            // send the late record
            sendMessage(58, ts - 4000, "late", producer)
            producer.flush()
          }
        }
      },
      delay,
      1000L
    )
  }

  def sendMessage(id: Long, ts: Long, info: String, producer: KafkaProducer[String, String]): Unit = {
    val window = (ts / 10000) * 10000
    val value = s"$window,$id,$info"
    producer.send(new ProducerRecord("late_event", null, ts, "$id", value)).get()
    log.info(s"Sent a record: $value")
  }

  def setup(props: Properties): Unit = {
    def admin = AdminClient.create(props)

    val topic1 = new NewTopic("late_event", 3, 3.toShort)
    val result = admin.createTopics(List(topic1).asJava)
    result.all().get(10, TimeUnit.SECONDS)
    admin.close()
  }
}
