package App.kafka_streams.Serdes;

import App.kafka_streams.models.Average;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;

public class AverageSerde implements Serde<Average> {
    @Override
    public Serializer<Average> serializer() {
        return (topic, data) -> {
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + Double.BYTES);
            buffer.putLong(data.getCount());
            buffer.putDouble(data.getTotal());
            return buffer.array();
        };
    }

    @Override
    public Deserializer<Average> deserializer() {
        return (topic, data) -> {
            ByteBuffer buffer = ByteBuffer.wrap(data);
            long count = buffer.getLong();
            double total = buffer.getDouble();
            return new Average(count, total);
        };
    }
}