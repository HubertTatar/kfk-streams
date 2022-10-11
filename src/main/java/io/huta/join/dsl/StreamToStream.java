package io.huta.join.dsl;

import io.huta.common.AdminConnectionProps;
import io.huta.common.SetupTopic;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class StreamToStream implements AdminConnectionProps, SetupTopic {

    private final String LEFT_STREAM = "left_stream";
    private final String RIGHT_STREAM = "right_stream";
    private final String OUTPUT_STREAM = "output_stream";

    public static void main(String[] args) {
        var join = new StreamToStream();
        join.setup();

    }

    void setup() {
        short replFact = 3;
        var admin = AdminClient.create(kfkProps());
        var topics = Stream.of(LEFT_STREAM, RIGHT_STREAM, OUTPUT_STREAM)
                .map(name -> new NewTopic(name, 3, replFact))
                .toList();
        var result = admin.createTopics(topics);
        try {
            result.all().get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            admin.close();
        }
    }


}
