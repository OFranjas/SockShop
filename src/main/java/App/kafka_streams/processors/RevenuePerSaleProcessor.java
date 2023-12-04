package App.kafka_streams.processors;

import App.kafka_streams.KafkaStreamProcessor;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RevenuePerSaleProcessor implements KafkaStreamProcessor {

    private static final Logger logger = LoggerFactory.getLogger(RevenuePerSaleProcessor.class);

    @Override
    public void process(KStream<String, String> salesStream, KStream<String, String> purchasesStream) {

        KStream<String, Double> revenuePerSaleStream = salesStream.mapValues(value -> {
            JSONObject saleJson = new JSONObject(value);
            double pricePerPair = saleJson.getDouble("pricePerPair");
            int numPairs = saleJson.getInt("numPairs");
            return pricePerPair * numPairs; // Calculate revenue
        });

        revenuePerSaleStream.foreach((key, revenue) -> logger
                .info("✅ REQ 5 -> Calculated Revenue for sale (Sale ID: {}): {}", key, revenue));
        revenuePerSaleStream.to("results_topic", Produced.with(Serdes.String(), Serdes.Double()));
    }
}
