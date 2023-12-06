package App.kafka_streams.processors;

import App.kafka_streams.KafkaStreamProcessor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import App.config.Config;

/**
 * The TotalRevenueProcessor class implements the KafkaStreamProcessor
 * interface. It processes a stream of sales data and calculates the total
 * revenue from all sales. The calculated total revenue is then sent to another
 * Kafka topic.
 */
public class TotalRevenueProcessor implements KafkaStreamProcessor {

    private static final Logger logger = LoggerFactory.getLogger(TotalRevenueProcessor.class);

    /**
     * The process method takes in a stream of sales data. It calculates the total
     * revenue from all sales by summing up the revenue from each sale. The
     * calculated
     * total revenue is logged and sent to the "results_topic" Kafka topic.
     *
     * @param salesStream     A stream of sales data.
     * @param purchasesStream A stream of purchases data (not used in this
     *                        processor).
     */
    @Override
    public void process(KStream<String, String> salesStream, KStream<String, String> purchasesStream) {

        // Convert sales data to revenue values
        KStream<String, Double> revenueStream = salesStream.mapValues(value -> {
            JSONObject sale = new JSONObject(value);
            double revenue = sale.getDouble("pricePerPair") * sale.getInt("numPairs");
            return revenue;
        });

        // Group the revenue stream by a null key to aggregate across all records
        revenueStream.groupBy(
                (key, value) -> "TotalRevenue",
                Grouped.with(Serdes.String(), Serdes.Double()))
                // Aggregate the revenues to calculate the total revenue
                .reduce(Double::sum, Materialized.with(Serdes.String(), Serdes.Double()))
                .toStream()
                .peek((key, totalRevenue) -> logger.info("✅ REQ 8 -> Total Revenue: {}", totalRevenue))
                .to(Config.RESULTS_TOPIC);
    }
}
