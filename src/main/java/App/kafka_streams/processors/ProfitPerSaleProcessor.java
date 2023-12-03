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

        // Create a KTable from the purchases stream
        KTable<String, String> expensesTable = purchasesStream.toTable();

        // Join the sales stream with the expenses KTable by a common key (e.g., sale ID)
        KStream<String, String> profitPerSaleStream = salesStream.join(
            expensesTable,
            (saleValue, expenseValue) -> {
                // Parse sale and expense records to JSON
                JSONObject sale = new JSONObject(saleValue);
                JSONObject expense = new JSONObject(expenseValue);

                // Calculate profit per sale
                double revenue = sale.getDouble("pricePerPair") * sale.getInt("numPairs");
                double expenseAmount = expense.getDouble("purchasePrice") * expense.getInt("quantity");
                double profit = revenue - expenseAmount;

                // Return the calculated profit as a string
                return String.valueOf(profit);
            }
        );

        // Log each calculated profit
        profitPerSaleStream.foreach((key, value) -> logger.info("✅ REQ 7 -> Calculated Profit for Sale (Sale ID: {}): {}", key, value));

        // Send the calculated profit to the results_topic
        profitPerSaleStream.to("results_topic");
    }
}
