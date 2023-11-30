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

/**
 * The KafkaStreamsApp class sets up and runs a Kafka Streams application.
 * It processes messages from both sales and purchases topics.
 */
public class KafkaStreamsApp {

    private static final Logger logger = LoggerFactory.getLogger(KafkaStreamsApp.class);

    /**
     * The main method sets up and starts the Kafka Streams application.
     * 
     * @param args Command line arguments (not used).
     */
    public static void main(String[] args) {

        // Kafka Streams configuration properties
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Building the stream processing topology
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> sourceStream = builder.stream(Arrays.asList("sales_topic", "purchases_topic"));

        // Processing each message in the stream
        sourceStream.foreach(new ForeachAction<String, String>() {
            @Override
            public void apply(String key, String value) {
                // Log the message for demonstration purposes
                logger.info("Message: {}", value);
            }
        });

        // Creating and starting the Kafka Streams application
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // Attaching a shutdown handler to gracefully close the Kafka Streams
        // application
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                // Closing the streams and logging the application closure
                streams.close();
                logger.info("Kafka Streams application closed");
            }
        }));
    }
}
