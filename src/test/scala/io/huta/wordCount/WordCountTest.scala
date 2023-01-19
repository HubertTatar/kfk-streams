package io.huta.wordCount

import org.apache.kafka.common.serialization.{LongDeserializer, Serdes, StringDeserializer, StringSerializer}
import org.apache.kafka.streams.{StreamsConfig, TopologyTestDriver}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.util.Properties
import scala.jdk.CollectionConverters._

class WordCountTest extends AnyWordSpec with Matchers with BeforeAndAfterAll {

  var testDriver: TopologyTestDriver = new TopologyTestDriver(WordCount.buildTopology(), testProps())

  "test" should {
    val testInputTopic = testDriver.createInputTopic(
      "words_to_count",
      new StringSerializer,
      new StringSerializer
    )
    val testOutputTopic = testDriver.createOutputTopic(
      "words_counted",
      new StringDeserializer(),
      new LongDeserializer()
    )
    "count words" in {
      testInputTopic.pipeInput("words to count")
      val list = testOutputTopic.readRecordsToList()
      list.size() shouldEqual 3
    }
  }

  def testProps() = {
    val props = new Properties
    props.putAll(
      Map(
        StreamsConfig.APPLICATION_ID_CONFIG -> "test",
        StreamsConfig.BOOTSTRAP_SERVERS_CONFIG -> "dummy",
        StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG -> Serdes.String().getClass,
        StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG -> Serdes.String().getClass
      ).asJava
    )
    props
  }

  override protected def afterAll(): Unit = {
    testDriver.close()
  }
}
