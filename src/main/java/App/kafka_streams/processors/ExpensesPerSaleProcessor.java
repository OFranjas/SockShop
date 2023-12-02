package App.kafka_streams.processors;

import App.kafka_streams.KafkaStreamProcessor;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.ForeachAction;
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
     * @param builder The StreamsBuilder for building Kafka Streams applications.
     */
    @Override
    public void process(StreamsBuilder builder) {
        // Create a stream from the purchases_topic
        KStream<String, String> purchasesStream = builder.stream("purchases_topic");

        // Map each purchase record to calculate expenses
        KStream<String, String> expensesPerSaleStream = purchasesStream.mapValues(new ValueMapper<String, String>() {
            @Override
            public String apply(String value) {
                // Parse the purchase record value to JSON
                JSONObject purchase = new JSONObject(value);

                // Extract purchase price and quantity from the purchase record
                double purchasePrice = purchase.getDouble("purchasePrice");
                int quantity = purchase.getInt("quantity");

                // Calculate the total expense for this purchase
                double expense = purchasePrice * quantity;

                // Return the calculated expense as a string
                return String.valueOf(expense);
            }
        });

        // Log each calculated expense
        expensesPerSaleStream.foreach(new ForeachAction<String, String>() {
            @Override
            public void apply(String key, String value) {
                // Log the calculated expense with a custom message format
                logger.info("✅ REQ 6 -> Calculated Expense: {}", value);
            }
        });

        // Optionally, send the calculated expenses to the results_topic
        expensesPerSaleStream.to("results_topic");

    }
}
