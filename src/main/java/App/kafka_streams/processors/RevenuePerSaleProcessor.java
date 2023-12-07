package App.kafka_streams.processors;

import App.kafka_streams.KafkaStreamProcessor;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import App.config.Config;

/**
 * The RevenuePerSaleProcessor class implements the KafkaStreamProcessor
 * interface.
 * It processes a stream of sales data and calculates the revenue for each sale.
 * The calculated revenue is then sent to another Kafka topic.
 */
public class RevenuePerSaleProcessor implements KafkaStreamProcessor {

    // Logger for logging information and calculated revenue
    private static final Logger logger = LoggerFactory.getLogger(RevenuePerSaleProcessor.class);

    /**
     * The process method takes in a stream of sales data and a stream of purchases
     * data.
     * It calculates the revenue for each sale by multiplying the price per pair
     * with the number of pairs.
     * The calculated revenue is logged and sent to the "results_topic" Kafka topic.
     *
     * @param salesStream     A stream of sales data.
     * @param purchasesStream A stream of purchases data.
     */
    @Override
    public void process(KStream<String, String> salesStream, KStream<String, String> purchasesStream) {

        // Transform the sales stream to calculate the revenue for each sale
        KStream<String, Double> revenuePerSaleStream = salesStream.mapValues(value -> {
            JSONObject saleJson = new JSONObject(value);
            double pricePerPair = saleJson.getDouble("pricePerPair");
            int numPairs = saleJson.getInt("numPairs");
            return pricePerPair * numPairs; // Calculate revenue
        });

        // Log the calculated revenue for each sale
        revenuePerSaleStream.foreach((key, revenue) -> logger
                .info("✅ REQ 5 -> Calculated Revenue for sale (Sock ID: {}): {}", key, String.format("%.2f", revenue)));

        KStream<String, String> formattedRevenuePerSaleStream = revenuePerSaleStream.mapValues(revenue -> {
            // Format revenue to have only two decimal places
            String formattedRevenue = String.format("%.2f", revenue);

            JSONObject json = new JSONObject();
            json.put("requirement_id", 5); // This is for requirement 5
            json.put("result", formattedRevenue);
            return json.toString();
        });

        // Send the formatted revenue stream to the "results_topic" Kafka topic
        formattedRevenuePerSaleStream.to(Config.RESULTS_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
    }
}
