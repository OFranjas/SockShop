package App.kafka_streams.processors;

import App.kafka_streams.KafkaStreamProcessor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

import App.config.Config;

/**
 * The HourlyProfitProcessor class implements the KafkaStreamProcessor
 * interface. It processes two streams of data - sales and purchases, and
 * calculates the total profit for each hour (a tumbling window of one hour).
 * The total profit for the latest window is then logged and sent to another
 * Kafka topic.
 */
public class HourlyProfitProcessor implements KafkaStreamProcessor {

        private static final Logger logger = LoggerFactory.getLogger(HourlyProfitProcessor.class);

        /**
         * The process method takes in two streams of data - sales and purchases.
         * It calculates the revenue for each sale by multiplying the price per pair
         * with the number of pairs. The revenues are then grouped by a common key
         * and windowed into one-hour segments using a tumbling window. The revenues
         * for each window are then aggregated to calculate the total revenue for that
         * window.
         *
         * Similarly, it calculates the expense for each purchase by multiplying the
         * purchase price with the quantity. The expenses are then grouped by the same
         * common key and windowed into one-hour segments using a tumbling window. The
         * expenses for each window are then aggregated to calculate the total expenses
         * for that window.
         *
         * The total profit for each window is then calculated by subtracting the total
         * expenses from the total revenue. The total profit for each window is then
         * logged
         * and sent to the "results_topic" Kafka topic.
         *
         * @param salesStream     A stream of sales data.
         * @param purchasesStream A stream of purchases data.
         */
        @Override
        public void process(KStream<String, String> salesStream, KStream<String, String> purchasesStream) {
                // Define a one-hour tumbling window
                Duration windowSize = Duration.ofHours(1);

                // Calculate total revenue for each window
                KTable<Windowed<String>, Double> windowedRevenue = salesStream.mapValues(value -> {
                        JSONObject sale = new JSONObject(value);
                        return sale.getDouble("pricePerPair") * sale.getInt("numPairs");
                })
                                .groupBy((key, revenue) -> "Total", Grouped.with(Serdes.String(), Serdes.Double()))
                                .windowedBy(TimeWindows.ofSizeWithNoGrace(windowSize))
                                .reduce(Double::sum, Materialized.with(Serdes.String(), Serdes.Double()));

                // Calculate total expenses for each window
                KTable<Windowed<String>, Double> windowedExpenses = purchasesStream.mapValues(value -> {
                        JSONObject purchase = new JSONObject(value);
                        return purchase.getDouble("purchasePrice") * purchase.getInt("quantity");
                })
                                .groupBy((key, expense) -> "Total", Grouped.with(Serdes.String(), Serdes.Double()))
                                .windowedBy(TimeWindows.ofSizeWithNoGrace(windowSize))
                                .reduce(Double::sum, Materialized.with(Serdes.String(), Serdes.Double()));

                // Calculate the profit in the last hour
                KTable<Windowed<String>, Double> windowedProfit = windowedRevenue.join(windowedExpenses,
                                (revenue, expense) -> revenue - expense);

                // Log the calculated total profit
                windowedProfit.toStream()
                                .peek((key, totalProfit) -> logger.info("✅ REQ 16 -> Profit in the last hour: {}",
                                                totalProfit));

                KStream<String, String> formattedTotalProfitStream = windowedProfit.toStream()
                                .map((key, totalProfit) -> new KeyValue<>(key.key(), totalProfit)) // Extract the
                                                                                                   // original key
                                .mapValues(totalProfit -> {
                                        // Format total profit to have only two decimal places
                                        String formattedTotalProfit = String.format("%.2f", totalProfit);

                                        JSONObject json = new JSONObject();
                                        json.put("requirement_id", 16); // This is for requirement 16
                                        json.put("result", formattedTotalProfit);
                                        return json.toString();
                                });

                // Send the formatted total profit stream to the "results_topic" Kafka topic
                formattedTotalProfitStream.to(Config.RESULTS_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
        }
}
