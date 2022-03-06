Examples:
- https://docs.confluent.io/platform/current/streams/code-examples.html
- https://github.com/apache/kafka/tree/trunk/streams/examples/src/main/java/org/apache/kafka/streams/examples
- https://github.com/confluentinc/kafka-streams-examples/tree/7.0.0-post/src/main/scala/io/confluent/examples/streams

Materials:
- https://www.confluent.io/blog/crossing-streams-joins-apache-kafka/
- https://docs.confluent.io/platform/current/streams/developer-guide/app-reset-tool.html
- https://www.confluent.io/blog/kafka-listeners-explained/
- https://www.confluent.io/blog/kafka-client-cannot-connect-to-broker-on-aws-on-docker-etc/

Guides:
- https://kafka.apache.org/31/documentation/streams/tutorial
- https://kafka.apache.org/31/documentation/streams/quickstart
- https://kafka.apache.org/20/documentation/streams/developer-guide/write-streams

Blogs:
- https://softwaremill.com/hands-on-kafka-streams-in-scala/

Commands:
    
    ./kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094 --topic words_counted --from-beginning --formatter kafka.tools.DefaultMessageFormatter --property print.key=true --property print.value=true --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer