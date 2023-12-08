package App.kafka_streams.processors;

import App.kafka_streams.KafkaStreamProcessor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

import App.config.Config;

/**
 * The HourlyRevenueProcessor class implements the KafkaStreamProcessor
 * interface. It processes a stream of sales data and calculates the total
 * revenue for each hour (a tumbling window of one hour). The total revenue for
 * the latest window is then logged and sent to another Kafka topic.
 */
public class HourlyRevenueProcessor implements KafkaStreamProcessor {

        private static final Logger logger = LoggerFactory.getLogger(HourlyRevenueProcessor.class);

        /**
         * The process method takes in a stream of sales data. It calculates the
         * revenue for each sale by multiplying the price per pair with the number
         * of pairs. The revenues are then grouped by key (sales ID) and windowed
         * into one-hour segments using a tumbling window. The revenues for each
         * window are then aggregated to calculate the total revenue for that window.
         * The total revenue for each window is then logged and sent to the
         * "results_topic" Kafka topic.
         *
         * @param salesStream     A stream of sales data.
         * @param purchasesStream A stream of purchases data (not used in this case).
         */
        @Override
        public void process(KStream<String, String> salesStream, KStream<String, String> purchasesStream) {
                // Define a one-hour tumbling window
                Duration windowSize = Duration.ofHours(1);

                // Convert sales data to revenue values and window the data
                KTable<Windowed<String>, Double> windowedRevenue = salesStream
                                .mapValues(value -> {
                                        JSONObject sale = new JSONObject(value);
                                        return sale.getDouble("pricePerPair") * sale.getInt("numPairs");
                                })
                                .groupBy((key, expense) -> "TotalRevenue",
                                                Grouped.with(Serdes.String(), Serdes.Double()))
                                .windowedBy(TimeWindows.ofSizeWithNoGrace(windowSize))
                                // Aggregate the revenues for each window to calculate the total revenue
                                .reduce(Double::sum, Materialized.with(Serdes.String(), Serdes.Double()));

                // Log the total revenue for the latest window
                windowedRevenue.toStream()
                                .peek((key, totalRevenue) -> logger
                                                .info("✅ REQ 14 -> Total Revenue in the last hour: {}", totalRevenue));

                KStream<String, String> formattedTotalRevenueStream = windowedRevenue.toStream()
                                .map((key, totalRevenue) -> new KeyValue<>(key.key(), totalRevenue)) // Extract the
                                                                                                     // original key
                                .mapValues(totalRevenue -> {
                                        // Format total revenue to have only two decimal places
                                        String formattedTotalRevenue = String.format("%.2f", totalRevenue);

                                        JSONObject json = new JSONObject();
                                        json.put("requirement_id", 14); // This is for requirement 14
                                        json.put("result", formattedTotalRevenue);
                                        return json.toString();
                                });

                // Send the formatted total revenue stream to the "results_topic" Kafka topic
                formattedTotalRevenueStream.to(Config.RESULTS_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
        }
}