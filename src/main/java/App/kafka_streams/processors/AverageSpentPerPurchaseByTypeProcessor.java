package App.kafka_streams.processors;

import App.kafka_streams.models.Average;
import App.kafka_streams.Serdes.AverageSerde;
import App.kafka_streams.KafkaStreamProcessor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import App.config.Config;

/**
 * The AverageSpentPerPurchaseByTypeProcessor class implements the
 * KafkaStreamProcessor
 * interface. It processes a stream of purchases data and calculates the average
 * amount spent per purchase by type. The calculated average is then sent to
 * another Kafka topic.
 */
public class AverageSpentPerPurchaseByTypeProcessor implements KafkaStreamProcessor {

        private static final Logger logger = LoggerFactory.getLogger(AverageSpentPerPurchaseByTypeProcessor.class);

        /**
         * The process method takes in a stream of purchases data. It calculates the
         * average
         * amount spent per purchase by type by summing up the expenses for each type
         * and dividing
         * by the number of purchases for each type. The calculated average is logged
         * and sent to the
         * "average_expense_per_type_topic" Kafka topic.
         *
         * @param salesStream     A stream of sales data (not used in this processor).
         * @param purchasesStream A stream of purchases data.
         */
        @Override
        public void process(KStream<String, String> salesStream, KStream<String, String> purchasesStream) {

                // Map each purchase to type and expense, then create an Average object
                KStream<String, Average> expensesStream = purchasesStream.map((key, value) -> {
                        JSONObject purchase = new JSONObject(value);
                        String type = purchase.getString("type");
                        double expense = purchase.getDouble("purchasePrice") * purchase.getInt("quantity");
                        return new org.apache.kafka.streams.KeyValue<>(type, new Average(1, expense));
                });

                // Aggregate total expenses and count per type
                KTable<String, Average> averageExpensesTable = expensesStream
                                .groupByKey(Grouped.with(Serdes.String(), new AverageSerde()))
                                .aggregate(
                                                () -> new Average(0L, 0.0),
                                                (key, value, aggregate) -> new Average(
                                                                aggregate.getCount() + value.getCount(),
                                                                aggregate.getTotal() + value.getTotal()),
                                                Materialized.with(Serdes.String(), new AverageSerde()));

                // Calculate and log the average expense per type
                averageExpensesTable.toStream()
                                .mapValues(avg -> avg.getTotal() / avg.getCount())
                                .peek((type, avgExpense) -> logger.info("✅ REQ 11 -> Average Expense for Type {}: {}",
                                                type, avgExpense));

                KStream<String, String> formattedAverageExpensesStream = averageExpensesTable.toStream()
                                .mapValues(avg -> avg.getTotal() / avg.getCount())
                                .mapValues(avgExpense -> {
                                        // Format average expense to have only two decimal places
                                        String formattedAvgExpense = String.format("%.2f", avgExpense);

                                        JSONObject json = new JSONObject();
                                        json.put("requirement_id", 11); // This is for requirement 11
                                        json.put("result", formattedAvgExpense);
                                        return json.toString();
                                });

                // Send the formatted average expense stream to the "results_topic" Kafka topic
                formattedAverageExpensesStream.to(Config.RESULTS_TOPIC,
                                Produced.with(Serdes.String(), Serdes.String()));
        }
}