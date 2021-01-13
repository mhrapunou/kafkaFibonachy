package by.epam.big_data.sevices;

import by.epam.big_data.config.SpringConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.Properties;

public class Streamer {
    public static void main(String[] args) {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(SpringConfig.class);
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "fibonacci-application");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());

        final int m = 5;

        StreamsBuilder builder = new StreamsBuilder();
        KStream<Long, Long> numbers = builder.stream("fibonacciInput");

        KTable<Long, Long> sums = numbers
                .groupBy((key, value) -> key  / m)
                .aggregate(() -> 0L, (key, value, aggValue) -> aggValue + value);

        sums.toStream().to("fibonacciOutput");



        KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        streams.start();
    }
}
