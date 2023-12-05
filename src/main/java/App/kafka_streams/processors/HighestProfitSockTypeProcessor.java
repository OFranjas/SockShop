package App.kafka_streams.processors;

import App.kafka_streams.Serdes.ProfitTypePairSerde;
import App.kafka_streams.models.ProfitTypePair;

import App.kafka_streams.KafkaStreamProcessor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HighestProfitSockTypeProcessor implements KafkaStreamProcessor {

    private static final Logger logger = LoggerFactory.getLogger(HighestProfitSockTypeProcessor.class);

    @Override
    public void process(KStream<String, String> salesStream, KStream<String, String> purchasesStream) {
        logger.info("Processing sales and purchase streams for highest profit by sock type");

        // Process sales stream to calculate revenue by type
        KTable<String, ProfitTypePair> salesByType = salesStream
                .mapValues(value -> {
                    JSONObject sale = new JSONObject(value);
                    ProfitTypePair revenue = new ProfitTypePair(
                            sale.getDouble("pricePerPair") * sale.getInt("numPairs"), sale.getString("type"));
                    return revenue;
                })
                .groupBy((key, value) -> value.getType(), Grouped.with(Serdes.String(), new ProfitTypePairSerde()))
                .aggregate(
                        () -> new ProfitTypePair(0.0, ""),
                        (key, value, aggregate) -> new ProfitTypePair(aggregate.getProfit() + value.getProfit(),
                                value.getType()),
                        Materialized.with(Serdes.String(), new ProfitTypePairSerde()));

        // Process purchases stream to calculate expenses by type
        KTable<String, ProfitTypePair> purchasesByType = purchasesStream
                .mapValues(value -> {
                    JSONObject purchase = new JSONObject(value);
                    ProfitTypePair expense = new ProfitTypePair(
                            purchase.getDouble("purchasePrice") * purchase.getInt("quantity"),
                            purchase.getString("type"));
                    return expense;
                })
                .groupBy((key, value) -> value.getType(), Grouped.with(Serdes.String(), new ProfitTypePairSerde()))
                .aggregate(
                        () -> new ProfitTypePair(0.0, ""),
                        (key, value, aggregate) -> new ProfitTypePair(aggregate.getProfit() + value.getProfit(),
                                value.getType()),
                        Materialized.with(Serdes.String(), new ProfitTypePairSerde()));

        // Calculate the profit per type
        KTable<String, Double> profitByType = salesByType.join(purchasesByType,
                (sales, purchases) -> {
                    double profit = sales.getProfit() - purchases.getProfit();
                    return profit;
                }, Materialized.with(Serdes.String(), Serdes.Double()));

        // Aggregate to find the type with the highest profit
        KTable<String, ProfitTypePair> maxProfitType = profitByType
                .groupBy((key, value) -> KeyValue.pair("maxProfit", new ProfitTypePair(value, key)),
                        Grouped.with(Serdes.String(), new ProfitTypePairSerde()))
                .reduce(
                        // Adder
                        (aggValue, newValue) -> {
                            if (newValue.getProfit() > aggValue.getProfit()) {
                                return newValue;
                            } else {
                                return aggValue;
                            }
                        },
                        // Subtractor (not used in this case, but required for method signature)
                        (aggValue, oldValue) -> aggValue);

        maxProfitType.toStream()
                .foreach((key, value) -> logger.info("✅ REQ 13 -> Type with Highest Profit: {} | Profit: {}",
                        value.getType(), value.getProfit()));

    }
}
