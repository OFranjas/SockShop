package App.kafka_streams.Serdes;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;
import App.kafka_streams.models.ProfitTypePair;

import java.util.Map;

public class ProfitTypePairSerde implements Serde<ProfitTypePair> {

    private static class ProfitTypePairSerializer implements Serializer<ProfitTypePair> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
        }

        @Override
        public byte[] serialize(String topic, ProfitTypePair data) {
            if (data == null)
                return null;
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("profit", data.getProfit());
            jsonObject.put("type", data.getType());
            return jsonObject.toString().getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public void close() {
        }
    }

    private static class ProfitTypePairDeserializer implements Deserializer<ProfitTypePair> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
        }

        @Override
        public ProfitTypePair deserialize(String topic, byte[] data) {
            if (data == null)
                return null;
            String dataString = new String(data, StandardCharsets.UTF_8);
            JSONObject jsonObject = new JSONObject(dataString);
            double profit = jsonObject.getDouble("profit");
            String type = jsonObject.getString("type");
            return new ProfitTypePair(profit, type);
        }

        @Override
        public void close() {
        }
    }

    @Override
    public Serializer<ProfitTypePair> serializer() {
        return new ProfitTypePairSerializer();
    }

    @Override
    public Deserializer<ProfitTypePair> deserializer() {
        return new ProfitTypePairDeserializer();
    }
}
