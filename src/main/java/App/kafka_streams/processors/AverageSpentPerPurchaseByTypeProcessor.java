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

public class AverageSpentPerPurchaseByTypeProcessor implements KafkaStreamProcessor {

        private static final Logger logger = LoggerFactory.getLogger(AverageSpentPerPurchaseByTypeProcessor.class);

        @Override
        public void process(KStream<String, String> salesStream, KStream<String, String> purchasesStream) {
                // Step 1: Map each purchase to type and expense, then create an Average object
                KStream<String, Average> expensesStream = purchasesStream
                                .map((key, value) -> {
                                        JSONObject purchase = new JSONObject(value);
                                        String type = purchase.getString("type");
                                        double expense = purchase.getDouble("purchasePrice")
                                                        * purchase.getInt("quantity");
                                        return new org.apache.kafka.streams.KeyValue<>(type, new Average(1, expense));
                                });

                // Step 2: Aggregate total expenses and count per type
                KTable<String, Average> averageExpensesTable = expensesStream
                                .groupByKey(Grouped.with(Serdes.String(), new AverageSerde()))
                                .aggregate(
                                                () -> new Average(0L, 0.0),
                                                (key, value, aggregate) -> new Average(
                                                                aggregate.getCount() + value.getCount(),
                                                                aggregate.getTotal() + value.getTotal()),
                                                Materialized.with(Serdes.String(), new AverageSerde()));

                // Step 3: Calculate and log the average expense per type
                averageExpensesTable.toStream()
                                .mapValues(avg -> avg.getTotal() / avg.getCount())
                                .peek((type, avgExpense) -> logger.info("✅ REQ 11 -> Average Expense for Type {}: {}",
                                                type, avgExpense))
                                .to("average_expense_per_type_topic", Produced.with(Serdes.String(), Serdes.Double()));
        }
}
