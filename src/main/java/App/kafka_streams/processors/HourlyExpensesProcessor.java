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

import App.config.Config;

/**
 * The HourlyExpensesProcessor class implements the KafkaStreamProcessor
 * interface. It processes a stream of purchase data and calculates the total
 * expenses for each hour (a tumbling window of one hour). The total expenses
 * for
 * the latest window are then logged and sent to another Kafka topic.
 */
public class HourlyExpensesProcessor implements KafkaStreamProcessor {

    private static final Logger logger = LoggerFactory.getLogger(HourlyExpensesProcessor.class);

    /**
     * The process method takes in a stream of purchase data. It calculates the
     * expense for each purchase by multiplying the purchase price with the
     * quantity.
     * The expenses are then grouped by key (purchase ID) and windowed
     * into one-hour segments using a tumbling window. The expenses for each
     * window are then aggregated to calculate the total expenses for that window.
     * The total expenses for each window are then logged and sent to the
     * "results_topic" Kafka topic.
     *
     * @param salesStream     A stream of sales data (not used in this case).
     * @param purchasesStream A stream of purchases data.
     */
    @Override
    public void process(KStream<String, String> salesStream, KStream<String, String> purchasesStream) {
        // Define a one-hour tumbling window
        Duration windowSize = Duration.ofHours(1);

        // Convert purchase data to expense values
        KTable<Windowed<String>, Double> windowedExpenses = purchasesStream.mapValues(value -> {
            JSONObject purchase = new JSONObject(value);
            return purchase.getDouble("purchasePrice") * purchase.getInt("quantity");
        })
                // Group and window the expense stream, then aggregate the values
                .groupBy((key, expense) -> "TotalExpenses", Grouped.with(Serdes.String(), Serdes.Double()))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(windowSize))
                .reduce(Double::sum, Materialized.with(Serdes.String(), Serdes.Double()));

        // Aggregate the total expenses for the latest window
        windowedExpenses.toStream()
                .peek((key, totalExpenses) -> logger.info("✅ REQ 15 -> Total Expenses in the last hour: {}",
                        totalExpenses))
                .to(Config.RESULTS_TOPIC);
    }
}