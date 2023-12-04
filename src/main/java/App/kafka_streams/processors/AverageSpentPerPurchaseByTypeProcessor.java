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

public class AverageSpentPerPurchaseByTypeProcessor implements KafkaStreamProcessor {

    private static final Logger logger = LoggerFactory.getLogger(AverageSpentPerPurchaseByTypeProcessor.class);

    @Override
    public void process(KStream<String, String> salesStream, KStream<String, String> purchasesStream) {
        // Extract type and then calculate expense
        KStream<String, Double> expensesStream = purchasesStream
                .map((key, value) -> {
                    JSONObject purchase = new JSONObject(value);
                    String type = purchase.getString("type");
                    return new org.apache.kafka.streams.KeyValue<>(type, value);
                })
                .mapValues(value -> {
                    JSONObject purchase = new JSONObject(value);
                    double purchasePrice = purchase.getDouble("purchasePrice");
                    int quantity = purchase.getInt("quantity");
                    return purchasePrice * quantity;
                });

        // Group the expense stream by sock type to aggregate across all records
        KGroupedStream<String, Double> groupedExpenses = expensesStream
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Double()));

        // Aggregate the expenses to calculate the total expenses per type
        KTable<String, Double> totalExpensesTable = groupedExpenses.reduce(Double::sum,
                Materialized.with(Serdes.String(), Serdes.Double()));

        // Count the number of purchases per type
        KTable<String, Long> countTable = expensesStream.groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
                .count(Materialized.with(Serdes.String(), Serdes.Long()));

        // Join the two tables to calculate the average per type
        KTable<String, Double> averageTable = totalExpensesTable.join(countTable,
                (totalExpenses, count) -> count == 0 ? 0.0 : totalExpenses / count);

        // Extract the resulting average as a KStream
        KStream<String, Double> averageStream = averageTable.toStream();

        // Log and send the average expenses per type
        averageStream
                .peek((key, avgExpense) -> logger.info("✅ REQ 11 -> Average Expense for Type {}: {}", key, avgExpense))
                .to("results_topic", Produced.with(Serdes.String(), Serdes.Double()));
    }
}
