package io.huta.joins.dsl

import io.huta.wordCount.WordCount
import org.apache.kafka.common.serialization.{
  IntegerDeserializer,
  IntegerSerializer,
  LongDeserializer,
  Serdes,
  StringDeserializer,
  StringSerializer
}
import org.apache.kafka.streams.kstream.JoinWindows
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.test.TestRecord
import org.apache.kafka.streams.{KeyValue, StreamsConfig, Topology, TopologyTestDriver}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.Duration
import java.util.Properties
import scala.jdk.CollectionConverters._

class StreamToStreamTest extends AnyWordSpec with Matchers with BeforeAndAfterAll {

  val leftTopicName = "left-topic"
  val rightTopicName = "right-topic"
  val outputTopicName = "output-topic"

  var testDriver: TopologyTestDriver = new TopologyTestDriver(leftJoinTopology(), testProps())

  "toplogy" should {
    val leftTopic = createInputTopic(leftTopicName)
    val rightTopic = createInputTopic(rightTopicName)
    val outputTopic = createOutputTopic(outputTopicName)

    "left join" in {
      leftTopic.pipeInput(1, "one")
      rightTopic.pipeInput(1, "one")
      val result = outputTopic.readRecord()

      assert(result.value() == "joined")
    }
  }

  def leftTestData() = List(
    new TestRecord(1, "one"),
    new TestRecord(2, "two"),
    new TestRecord(3, "three")
  )

  def rightTestData() = List(
    new TestRecord(2, "two"),
    new TestRecord(3, "three"),
    new TestRecord(4, "four")
  )

  def leftJoinTopology(): Topology = {
    import org.apache.kafka.streams.scala.ImplicitConversions._
    import org.apache.kafka.streams.scala.serialization.Serdes._
    val builder = new StreamsBuilder
    val leftStream = builder.stream[Int, String](leftTopicName)
    val rightStream = builder.stream[Int, String](rightTopicName)

    leftStream
      .leftJoin(rightStream)(
        (left, right) => if (right == null) "not-joined" else "joined",
        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(10))
      )
      .to(outputTopicName)

    builder.build()
  }

  def createOutputTopic(name: String) =
    testDriver.createOutputTopic(name, new IntegerDeserializer, new StringDeserializer)

  def createInputTopic(name: String) = testDriver.createInputTopic(name, new IntegerSerializer(), new StringSerializer)

  def testProps() = {
    val props = new Properties
    props.putAll(
      Map(
        StreamsConfig.APPLICATION_ID_CONFIG -> "test",
        StreamsConfig.BOOTSTRAP_SERVERS_CONFIG -> "dummy",
        StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG -> Serdes.Integer().getClass,
        StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG -> Serdes.String().getClass
      ).asJava
    )
    props
  }

  override protected def afterAll(): Unit = {
    testDriver.close()
  }
}
