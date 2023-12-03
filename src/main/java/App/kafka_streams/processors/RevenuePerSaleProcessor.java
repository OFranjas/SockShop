package App.kafka_streams.processors;

import App.kafka_streams.KafkaStreamProcessor;

import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Processor for calculating the revenue per sock pair sale.
 * Implements the KafkaStreamProcessor interface.
 */
public class RevenuePerSaleProcessor implements KafkaStreamProcessor {

    // Logger for logging information
    private static final Logger logger = LoggerFactory.getLogger(RevenuePerSaleProcessor.class);

    /**
     * Processes the sales data from the sales_topic to calculate the revenue per
     * sale.
     * 
     * @param builder The StreamsBuilder for building Kafka Streams applications.
     */
    @Override
    public void process(KStream<String, String> salesStream, KStream<String, String> purchasesStream) {

        // Map each sale record to calculate revenue
        KStream<String, String> revenuePerSaleStream = salesStream.mapValues(new ValueMapper<String, String>() {
            @Override
            public String apply(String value) {
                // Parse the sale record value to JSON
                JSONObject sale = new JSONObject(value);

                // Extract price per pair and number of pairs from the sale record
                double pricePerPair = sale.getDouble("pricePerPair");
                int numPairs = sale.getInt("numPairs");

                // Calculate the total revenue for this sale
                double revenue = pricePerPair * numPairs;

                // Return the calculated revenue as a string
                return String.valueOf(revenue);
            }
        });

        // Log each calculated revenue
        revenuePerSaleStream.foreach(new ForeachAction<String, String>() {
            @Override
            public void apply(String key, String value) {
                // Log the calculated revenue with a custom message format
                logger.info("✅ REQ 5 -> Calculated Revenue for sale (Sale ID: {}): {}", key, value);
            }
        });

        // Send the calculated revenue to the results_topic
        revenuePerSaleStream.to("results_topic");
    }
}
