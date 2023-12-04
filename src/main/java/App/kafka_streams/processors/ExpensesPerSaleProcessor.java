package App.kafka_streams.processors;

import App.kafka_streams.KafkaStreamProcessor;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Processor for calculating the expenses per sock pair sale.
 */
public class ExpensesPerSaleProcessor implements KafkaStreamProcessor {

    private static final Logger logger = LoggerFactory.getLogger(ExpensesPerSaleProcessor.class);

    /**
     * Processes the purchases data from the purchases_topic to calculate the
     * expenses per purchase.
     * 
     * @param salesStream The stream of sales data.
     * @param purchasesStream The stream of purchases data.
     */
    @Override
    public void process(KStream<String, String> salesStream, KStream<String, String> purchasesStream) {

        // Map each purchase record to calculate expenses
        KStream<String, String> expensesPerSaleStream = purchasesStream.mapValues((ValueMapper<String, String>) value -> {
            JSONObject purchase = new JSONObject(value);
            double purchasePrice = purchase.getDouble("purchasePrice");
            int quantity = purchase.getInt("quantity");
            return String.valueOf(purchasePrice * quantity);
        });

        // Log each calculated expense
        expensesPerSaleStream.foreach((key, value) -> logger.info("✅ REQ 6 -> Calculated Expense for purchase (Purchase ID: {}): {}", key, value));

        // Send the calculated expenses to the results_topic
        expensesPerSaleStream.to("results_topic");
    }
}
