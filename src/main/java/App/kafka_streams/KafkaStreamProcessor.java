package App.kafka_streams;

import org.apache.kafka.streams.kstream.KStream;

public interface KafkaStreamProcessor {
    void process(KStream<String, String> salesStream, KStream<String, String> purchasesStream);

}
