package App.kafka_streams.processors;

import App.kafka_streams.KafkaStreamProcessor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import App.config.Config;

/**
 * The AverageSpentPerPurchaseProcessor class implements the
 * KafkaStreamProcessor
 * interface. It processes a stream of purchases data and calculates the average
 * amount spent per purchase. The calculated average is then sent to another
 * Kafka topic.
 */
public class AverageSpentPerPurchaseProcessor implements KafkaStreamProcessor {

        private static final Logger logger = LoggerFactory.getLogger(AverageSpentPerPurchaseProcessor.class);

        /**
         * The process method takes in a stream of purchases data. It calculates the
         * average
         * amount spent per purchase by summing up the expenses for each purchase and
         * dividing
         * by the number of purchases. The calculated average is logged and sent to the
         * "results_topic" Kafka topic.
         *
         * @param salesStream     A stream of sales data (not used in this processor).
         * @param purchasesStream A stream of purchases data.
         */
        @Override
        public void process(KStream<String, String> salesStream, KStream<String, String> purchasesStream) {

                // Convert purchase data to expense values
                KStream<String, Double> expensesStream = purchasesStream.mapValues(value -> {
                        JSONObject purchase = new JSONObject(value);
                        double purchasePrice = purchase.getDouble("purchasePrice");
                        int quantity = purchase.getInt("quantity");
                        return purchasePrice * quantity;
                });

                // Group the expense stream by a constant key to aggregate across all records
                KGroupedStream<String, Double> groupedExpenses = expensesStream.groupBy(
                                (key, value) -> "total",
                                Grouped.with(Serdes.String(), Serdes.Double()));

                // Aggregate the expenses to calculate the total expenses
                KTable<String, Double> totalExpensesTable = groupedExpenses
                                .reduce(Double::sum, Materialized.with(Serdes.String(), Serdes.Double()));

                // Count the number of purchases
                KTable<String, Long> countTable = purchasesStream.groupBy(
                                (key, value) -> "total",
                                Grouped.with(Serdes.String(), Serdes.String()))
                                .count(Materialized.with(Serdes.String(), Serdes.Long()));

                // Join the two tables to calculate the average
                KTable<String, Double> averageTable = totalExpensesTable.join(
                                countTable,
                                (totalExpenses, count) -> count == 0 ? 0.0 : totalExpenses / count);

                // Log the calculated average expense per purchase
                averageTable.toStream()
                                .mapValues(avgExpense -> avgExpense)
                                .foreach((key, avgExpense) -> logger.info("✅ REQ 12 -> Average Expense: {}",
                                                avgExpense));

                KStream<String, String> formattedAverageExpensesStream = averageTable.toStream()
                                .mapValues(avgExpense -> {
                                        // Format average expense to have only two decimal places
                                        String formattedAvgExpense = String.format("%.2f", avgExpense);

                                        JSONObject json = new JSONObject();
                                        json.put("requirement_id", 12); // This is for requirement 12
                                        json.put("result", formattedAvgExpense);
                                        return json.toString();
                                });

                // Send the formatted average expense stream to the "results_topic" Kafka topic
                formattedAverageExpensesStream.to(Config.RESULTS_TOPIC,
                                Produced.with(Serdes.String(), Serdes.String()));
        }
}
