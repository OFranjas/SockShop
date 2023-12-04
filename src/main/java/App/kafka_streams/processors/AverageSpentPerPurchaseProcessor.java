package App.kafka_streams.processors;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import App.kafka_streams.KafkaStreamProcessor;

public class AverageSpentPerPurchaseProcessor implements KafkaStreamProcessor {

    private static final Logger logger = LoggerFactory.getLogger(AverageSpentPerPurchaseProcessor.class);

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
                (totalExpenses, count) -> count == 0 ? 0 : totalExpenses / count);

        // Extract the resulting average as a KStream
        KStream<String, Double> averageStream = averageTable.toStream();

        // Use peek for logging
        averageStream.peek((key, avgExpense) -> logger.info("✅ REQ 12 -> Average Expense: {}", avgExpense));

        // Send the result to a topic
        averageStream.to("results_topic", Produced.with(Serdes.String(), Serdes.Double()));
    }
}