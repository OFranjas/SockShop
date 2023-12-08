package App.kafka_streams.processors;

import App.kafka_streams.KafkaStreamProcessor;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import App.config.Config;

/**
 * The ExpensesPerSaleProcessor class implements the KafkaStreamProcessor
 * interface.
 * It processes a stream of purchases data and calculates the expenses for each
 * purchase.
 * The calculated expenses are then sent to another Kafka topic.
 */
public class ExpensesPerSaleProcessor implements KafkaStreamProcessor {

    // Logger for logging information and calculated expenses
    private static final Logger logger = LoggerFactory.getLogger(ExpensesPerSaleProcessor.class);

    /**
     * The process method takes in a stream of sales data and a stream of purchases
     * data.
     * It calculates the expenses for each purchase by multiplying the purchase
     * price with the quantity.
     * The calculated expenses are logged and sent to the "results_topic" Kafka
     * topic.
     *
     * @param salesStream     A stream of sales data.
     * @param purchasesStream A stream of purchases data.
     */
    @Override
    public void process(KStream<String, String> salesStream, KStream<String, String> purchasesStream) {

        // Transform the purchases stream to calculate the expenses for each purchase
        KStream<String, Double> expensesPerSaleStream = purchasesStream
                .mapValues((ValueMapper<String, Double>) value -> {
                    JSONObject purchase = new JSONObject(value);
                    double purchasePrice = purchase.getDouble("purchasePrice");
                    int quantity = purchase.getInt("quantity");
                    return purchasePrice * quantity; // Calculate expenses
                });

        expensesPerSaleStream.foreach((key, expense) -> logger
                .info("✅ REQ 6 -> Calculated Expense for purchase (Purchase ID: {}): {}", key,
                        String.format("%.2f", expense)));

        KStream<String, String> formattedExpensesPerSaleStream = expensesPerSaleStream.mapValues(expense -> {
            // Format expense to have only two decimal places
            String formattedExpense = String.format("%.2f", expense);

            JSONObject json = new JSONObject();
            json.put("requirement_id", 6); // This is for requirement 6
            json.put("result", formattedExpense);
            return json.toString();
        });

        // Send the formatted expenses stream to the "results_topic" Kafka topic
        formattedExpensesPerSaleStream.to(Config.RESULTS_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
    }
}
