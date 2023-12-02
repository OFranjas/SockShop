package App.kafka_streams;

import org.apache.kafka.streams.StreamsBuilder;

public interface KafkaStreamProcessor {
    void process(StreamsBuilder builder);
}
