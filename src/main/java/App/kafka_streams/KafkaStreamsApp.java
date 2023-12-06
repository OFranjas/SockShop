package App.kafka_streams;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.common.serialization.Serdes;
import java.util.Properties;

import App.config.Config;

import App.kafka_streams.processors.*;

public class KafkaStreamsApp {

    public static void main(String[] args) {

        // Properties and StreamsBuilder setup
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, Config.APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Config.KAFKA_BROKER);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();

        // Create a stream from the sales_topic
        KStream<String, String> salesStream = builder.stream(Config.SALES_TOPIC);

        // Create a stream from the purchases_topic
        KStream<String, String> purchasesStream = builder.stream(Config.PURCHASES_TOPIC);

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
        // totalRevenueProcessor.process(salesStream, null);

        // * REQ 9 -> Total expenses processor
        KafkaStreamProcessor totalExpensesProcessor = new TotalExpensesProcessor();
        // totalExpensesProcessor.process(null, purchasesStream);

        // * REQ 10 -> Total profit processor
        KafkaStreamProcessor totalProfitProcessor = new TotalProfitProcessor();
        // totalProfitProcessor.process(salesStream, purchasesStream);

        // * REQ 11 -> Average spent per purchase type processor
        KafkaStreamProcessor averageSpentPerPurchaseByTypeProcessor = new AverageSpentPerPurchaseByTypeProcessor();
        // averageSpentPerPurchaseByTypeProcessor.process(salesStream, purchasesStream);

        // * REQ 12 -> Average amount spent per purchase processor
        KafkaStreamProcessor averageSpentPerPurchaseProcessor = new AverageSpentPerPurchaseProcessor();
        // averageSpentPerPurchaseProcessor.process(salesStream, purchasesStream);

        // * REQ 13 -> Highest profit sock type processor
        KafkaStreamProcessor highestProfitSockTypeProcessor = new HighestProfitSockTypeProcessor();
        // highestProfitSockTypeProcessor.process(salesStream, purchasesStream);

        // * REQ 14 -> Hourly revenue processor
        KafkaStreamProcessor hourlyRevenueProcessor = new HourlyRevenueProcessor();
        // hourlyRevenueProcessor.process(salesStream, purchasesStream);

        // * REQ 15 -> Hourly expenses processor
        KafkaStreamProcessor hourlyExpensesProcessor = new HourlyExpensesProcessor();
        // hourlyExpensesProcessor.process(salesStream, purchasesStream);

        // * REQ 16 -> Hourly profit processor
        KafkaStreamProcessor hourlyProfitProcessor = new HourlyProfitProcessor();
        hourlyProfitProcessor.process(salesStream, purchasesStream);

        // Build and start the Kafka Streams application
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.cleanUp();
        streams.start();

        // Add shutdown hook for gracefully closing the application
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Detectado pedido de desligamento, encerrando o Kafka Streams.");
            streams.close();
        }));

        // Keep the application alive until it's manually stopped or receives a shutdown
        // signal
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

}
