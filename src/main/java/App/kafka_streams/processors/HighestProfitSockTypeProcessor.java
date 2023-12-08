package App.kafka_streams.processors;

import App.kafka_streams.Serdes.ProfitTypePairSerde;
import App.kafka_streams.models.ProfitTypePair;
import App.kafka_streams.KafkaStreamProcessor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import App.config.Config;

/**
 * The HighestProfitSockTypeProcessor class implements the KafkaStreamProcessor
 * interface. It processes a stream of sales data and a stream of purchases
 * data. It calculates the profit for each sock type and finds the sock type
 * with the highest profit.
 * The sock type with the highest profit is then logged and sent to another
 * Kafka topic.
 */
public class HighestProfitSockTypeProcessor implements KafkaStreamProcessor {

        private static final Logger logger = LoggerFactory.getLogger(HighestProfitSockTypeProcessor.class);

        /**
         * The process method takes in a stream of sales data and a stream of purchases
         * data. It calculates the profit for each sock type by subtracting the expenses
         * from the
         * revenue for each sock type. It then finds the sock type with the highest
         * profit.
         * The sock type with the highest profit is logged and sent to the
         * "results_topic" Kafka topic.
         *
         * @param salesStream     A stream of sales data.
         * @param purchasesStream A stream of purchases data.
         */
        @Override
        public void process(KStream<String, String> salesStream, KStream<String, String> purchasesStream) {

                // Process sales stream to calculate revenue by type
                KTable<String, ProfitTypePair> salesByType = salesStream.mapValues(value -> {
                        JSONObject sale = new JSONObject(value);
                        ProfitTypePair revenue = new ProfitTypePair(
                                        sale.getDouble("pricePerPair") * sale.getInt("numPairs"),
                                        sale.getString("type"));
                        return revenue;
                }).groupBy((key, value) -> value.getType(), Grouped.with(Serdes.String(), new ProfitTypePairSerde()))
                                .aggregate(
                                                () -> new ProfitTypePair(0.0, ""),
                                                (key, value, aggregate) -> new ProfitTypePair(
                                                                aggregate.getProfit() + value.getProfit(),
                                                                value.getType()),
                                                Materialized.with(Serdes.String(), new ProfitTypePairSerde()));

                // Process purchases stream to calculate expenses by type
                KTable<String, ProfitTypePair> purchasesByType = purchasesStream.mapValues(value -> {
                        JSONObject purchase = new JSONObject(value);
                        ProfitTypePair expense = new ProfitTypePair(
                                        purchase.getDouble("purchasePrice") * purchase.getInt("quantity"),
                                        purchase.getString("type"));
                        return expense;
                }).groupBy((key, value) -> value.getType(), Grouped.with(Serdes.String(), new ProfitTypePairSerde()))
                                .aggregate(
                                                () -> new ProfitTypePair(0.0, ""),
                                                (key, value, aggregate) -> new ProfitTypePair(
                                                                aggregate.getProfit() + value.getProfit(),
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

                // Log the type with the highest profit
                maxProfitType.toStream()
                                .mapValues(value -> value)
                                .foreach((key, value) -> logger.info(
                                                "✅ REQ 13 -> Type with Highest Profit: {} | Profit: {}",
                                                value.getType(), value.getProfit()));

                // Send the type with the highest profit to the results topic
                maxProfitType.toStream()
                                .mapValues(value -> {
                                        JSONObject json = new JSONObject();
                                        json.put("requirement_id", 13); // This is for requirement 13
                                        json.put("result", value.getType());
                                        return json.toString();
                                })
                                .to(Config.RESULTS_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
        }
}