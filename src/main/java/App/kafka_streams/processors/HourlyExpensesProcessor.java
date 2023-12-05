package App.kafka_streams.processors;

import App.kafka_streams.KafkaStreamProcessor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.Grouped;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class HourlyExpensesProcessor implements KafkaStreamProcessor {

    private static final Logger logger = LoggerFactory.getLogger(HourlyExpensesProcessor.class);

    @Override
    public void process(KStream<String, String> salesStream, KStream<String, String> purchasesStream) {
        // Define a one-hour tumbling window
        Duration windowSize = Duration.ofHours(1);

        // Convert purchase data to expense values
        KStream<String, Double> expensesStream = purchasesStream.mapValues(value -> {
            JSONObject purchase = new JSONObject(value);
            return purchase.getDouble("purchasePrice") * purchase.getInt("quantity");
        });

        // Group and window the expense stream, then aggregate the values
        KTable<Windowed<String>, Double> windowedExpenses = expensesStream
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(windowSize))
                .reduce(Double::sum, Materialized.with(Serdes.String(), Serdes.Double()));

        // Log the total expenses for each window
        windowedExpenses.toStream()
                .peek((key, totalExpenses) -> logger.info("✅ REQ 15 -> Total Expenses in window {}: {}",
                        key.window().toString(), totalExpenses))
                .to("results_topic");
    }
}
