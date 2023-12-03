package App.kafka_streams;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.common.serialization.Serdes;
import java.util.Properties;

import App.kafka_streams.processors.ExpensesPerSaleProcessor;
import App.kafka_streams.processors.RevenuePerSaleProcessor;
import App.kafka_streams.processors.ProfitPerSaleProcessor;
import App.kafka_streams.processors.TotalRevenueProcessor;

public class KafkaStreamsApp {

    public static void main(String[] args) {

        // Properties and StreamsBuilder setup
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();

        // Create a stream from the sales_topic
        KStream<String, String> salesStream = builder.stream("sales_topic");

        // Create a stream from the purchases_topic
        KStream<String, String> purchasesStream = builder.stream("purchases_topic");

        // * REQ 5 -> revenue per sale processor
        KafkaStreamProcessor revenuePerSaleProcessor = new RevenuePerSaleProcessor();
        // revenuePerSaleProcessor.process(salesStream, null);

        // * REQ 6 -> Expenses per sale processor
        KafkaStreamProcessor expensesPerSaleProcessor = new ExpensesPerSaleProcessor();
        // expensesPerSaleProcessor.process(null, purchasesStream);

        // * REQ 7 -> Profit per sale processor
        KafkaStreamProcessor profitPerSaleProcessor = new ProfitPerSaleProcessor();
        // profitPerSaleProcessor.process(salesStream, purchasesStream);

        // * REQ 8 -> Total revenue processor
        KafkaStreamProcessor totalRevenueProcessor = new TotalRevenueProcessor();
        totalRevenueProcessor.process(salesStream, null);

        // Build and start the Kafka Streams application
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // Add shutdown hook for gracefully closing the application
        // Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
        //     @Override
        //     public void run() {
        //         streams.close();
        //     }
        // }));
    }
}
