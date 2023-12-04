package App.kafka_streams.processors;

import App.kafka_streams.KafkaStreamProcessor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TotalRevenueProcessor implements KafkaStreamProcessor {

    private static final Logger logger = LoggerFactory.getLogger(TotalRevenueProcessor.class);

    @Override
    public void process(KStream<String, String> salesStream, KStream<String, String> purchasesStream) {
        // Convert sales data to revenue values
        KStream<String, Double> revenueStream = salesStream.mapValues(value -> {
            JSONObject sale = new JSONObject(value);
            return sale.getDouble("pricePerPair") * sale.getInt("numPairs");
        });

        // Group the revenue stream by a constant key to aggregate across all records
        KGroupedStream<String, Double> groupedRevenue = revenueStream.groupBy(
                (key, value) -> "total",
                Grouped.with(Serdes.String(), Serdes.Double()));

        // Aggregate the revenues to calculate the total revenue
        groupedRevenue
                .reduce(Double::sum, Materialized.with(Serdes.String(), Serdes.Double()))
                .toStream()
                .peek((key, totalRevenue) -> logger.info("✅ REQ 8 -> Total Revenue: {}", totalRevenue))
                .to("results_topic");
    }
}
