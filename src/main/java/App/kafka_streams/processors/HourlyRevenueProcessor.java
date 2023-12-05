package App.kafka_streams.processors;

import App.kafka_streams.KafkaStreamProcessor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class HourlyRevenueProcessor implements KafkaStreamProcessor {

    private static final Logger logger = LoggerFactory.getLogger(HourlyRevenueProcessor.class);

    @Override
    public void process(KStream<String, String> salesStream, KStream<String, String> purchasesStream) {
        // Define a one-hour tumbling window
        Duration windowSize = Duration.ofHours(1);

        // Convert sales data to revenue values
        KStream<String, Double> revenueStream = salesStream.mapValues(value -> {
            JSONObject sale = new JSONObject(value);
            return sale.getDouble("pricePerPair") * sale.getInt("numPairs");
        });

        // Group and window the revenue stream, then aggregate the values
        TimeWindowedKStream<String, Double> windowedRevenueStream = revenueStream
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(windowSize));

        // Aggregate the revenues for each window to calculate the total revenue
        KTable<Windowed<String>, Double> windowedRevenue = windowedRevenueStream
                .reduce(Double::sum, Materialized.with(Serdes.String(), Serdes.Double()));

        // Log the total revenue for each window
        windowedRevenue.toStream()
                .peek((key, totalRevenue) -> logger.info("✅ REQ 14 -> Total Revenue in window {}: {}",
                        key.window().toString(), totalRevenue))
                .to("results_topic");
    }
}
