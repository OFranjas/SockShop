package App.kafka_streams.processors;

import App.kafka_streams.KafkaStreamProcessor;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Processor for calculating the profit per sock pair sale.
 */
public class ProfitPerSaleProcessor implements KafkaStreamProcessor {

    private static final Logger logger = LoggerFactory.getLogger(ProfitPerSaleProcessor.class);

    @Override
    public void process(KStream<String, String> salesStream, KStream<String, String> purchasesStream) {

        // Create a KTable from the sales stream
        KTable<String, String> salesTable = salesStream.toTable();

        // Create a KTable from the purchases stream
        KTable<String, String> expensesTable = purchasesStream.toTable();

        // Join the sales stream with the expenses KTable, calculating the profit per
        // each sale
        KStream<String, String> profitPerSaleStream = salesStream.join(
                expensesTable,
                (saleValue, expenseValue) -> calculateProfit(saleValue, expenseValue));

        // Log and send the result
        profitPerSaleStream
                .peek((key, value) -> logger.info("✅ REQ 7 -> Calculated Profit for Sale (Sale ID: {}): {}", key,
                        value))
                .to("results_topic");
    }

    private String calculateProfit(String saleValue, String expenseValue) {
        // Fallback logic if the expenseValue is null
        if (expenseValue == null) {
            logger.warn("Missing Expense data for Sale: {}", saleValue);
            return "0"; // or some other default/fallback logic
        }

        JSONObject sale = new JSONObject(saleValue);
        JSONObject expense = new JSONObject(expenseValue);

        double revenue = sale.getDouble("pricePerPair") * sale.getInt("numPairs");
        double expenseAmount = expense.getDouble("purchasePrice") * expense.getInt("quantity");
        return String.valueOf(revenue - expenseAmount);
    }
}
