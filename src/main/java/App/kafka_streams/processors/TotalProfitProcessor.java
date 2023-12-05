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
        // Convert sales stream to a KTable
        KTable<String, String> salesTable = salesStream.toTable();

        // Convert purchases stream to a KTable
        KTable<String, String> expensesTable = purchasesStream.toTable();

        // Join the sales and expenses tables to calculate profit for each sale
        KTable<String, Double> profitPerSaleTable = salesTable.join(
                expensesTable,
                (saleValue, expenseValue) -> calculateProfit(saleValue, expenseValue));

        // Group the profit table by a constant key to aggregate across all records
        KGroupedStream<String, Double> groupedProfit = profitPerSaleTable.toStream().groupBy(
                (key, value) -> "total",
                Grouped.with(Serdes.String(), Serdes.Double()));

        // Aggregate the profits to calculate the total profit
        groupedProfit
                .reduce(Double::sum, Materialized.with(Serdes.String(), Serdes.Double()))
                .toStream()
                .peek((key, totalProfit) -> logger.info("✅ REQ 10 -> Total Profit: {}", totalProfit))
                .to("results_topic");
    }

    private Double calculateProfit(String saleValue, String expenseValue) {
        // Fallback logic if either saleValue or expenseValue is null
        if (saleValue == null || expenseValue == null) {
            logger.warn("Missing data for Profit Calculation");
            return 0.0; // or some other default/fallback logic
        }

        JSONObject sale = new JSONObject(saleValue);
        JSONObject expense = new JSONObject(expenseValue);

        double revenue = sale.getDouble("pricePerPair") * sale.getInt("numPairs");
        double expenseAmount = expense.getDouble("purchasePrice") * expense.getInt("quantity");
        return revenue - expenseAmount;
    }
}
