package App.kafka_streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

public class KafkaStreamsApp {

    private static final Logger logger = LoggerFactory.getLogger(KafkaStreamsApp.class);

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> sourceStream = builder.stream(Arrays.asList("sales_topic", "purchases_topic"));
        
        sourceStream.foreach(new ForeachAction<String, String>() {
            @Override
            public void apply(String key, String value) {
                logger.info("Message: {}", value);
            }
        });

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // Attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                streams.close();
                logger.info("Kafka Streams application closed");
            }
        }));
    }
}
