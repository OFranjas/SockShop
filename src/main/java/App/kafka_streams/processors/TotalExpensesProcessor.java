package App.kafka_streams.processors;

import App.kafka_streams.KafkaStreamProcessor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TotalExpensesProcessor implements KafkaStreamProcessor {

    private static final Logger logger = LoggerFactory.getLogger(TotalExpensesProcessor.class);

    @Override
    public void process(KStream<String, String> salesStream, KStream<String, String> purchasesStream) {
        // Convert purchase data to expense values
        KStream<String, Double> expensesStream = purchasesStream.mapValues((ValueMapper<String, Double>) value -> {
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
        groupedExpenses
                .reduce(Double::sum, Materialized.with(Serdes.String(), Serdes.Double()))
                .toStream()
                .foreach((key, totalExpenses) -> logger.info("✅ REQ 9 -> Total Expenses: {}", totalExpenses));

        groupedExpenses
                .reduce(Double::sum, Materialized.with(Serdes.String(), Serdes.Double()))
                .toStream()
                .to("results_topic");
    }
}