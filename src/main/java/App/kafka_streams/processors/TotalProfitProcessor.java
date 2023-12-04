package App.kafka_streams.processors;

import App.kafka_streams.KafkaStreamProcessor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TotalProfitProcessor implements KafkaStreamProcessor {

    private static final Logger logger = LoggerFactory.getLogger(TotalProfitProcessor.class);

    @Override
    public void process(KStream<String, String> salesStream, KStream<String, String> purchasesStream) {
        // Create a KTable from the purchases stream
        KTable<String, String> expensesTable = purchasesStream.toTable();

        // Join the sales stream with the expenses KTable to calculate profit for each sale
        KStream<String, Double> profitPerSaleStream = salesStream.join(
            expensesTable,
            (saleValue, expenseValue) -> {
                JSONObject sale = new JSONObject(saleValue);
                JSONObject expense = new JSONObject(expenseValue);

                double revenue = sale.getDouble("pricePerPair") * sale.getInt("numPairs");
                double expenseAmount = expense.getDouble("purchasePrice") * expense.getInt("quantity");
                return revenue - expenseAmount;
            });

        // Group the profit stream by a constant key to aggregate across all records
        KGroupedStream<String, Double> groupedProfit = profitPerSaleStream.groupBy(
                (key, value) -> "total",
                Grouped.with(Serdes.String(), Serdes.Double()));

        // Aggregate the profits to calculate the total profit
        groupedProfit
                .reduce(Double::sum, Materialized.with(Serdes.String(), Serdes.Double()))
                .toStream()
                .peek((key, totalProfit) -> logger.info("✅ REQ 10 -> Total Profit: {}", totalProfit))
                .to("results_topic");
    }
}
