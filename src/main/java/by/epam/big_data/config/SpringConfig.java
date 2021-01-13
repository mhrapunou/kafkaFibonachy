package by.epam.big_data.config;


import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanCustomizer;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;

import java.util.HashMap;
import java.util.Map;

@Configuration
@ComponentScan("by.epam.big_data")
@PropertySource("classpath:producerConfig.properties")
@EnableKafka
@EnableKafkaStreams
public class SpringConfig {

    @Autowired
    Environment environment;

    @Bean
    public KafkaAdmin admin(){
        Map<String,Object>configs= new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,environment.getProperty("bootstrap.servers"));
        return new KafkaAdmin(configs);}

    @Bean
    public NewTopic topicInput() {
        return TopicBuilder.name("fibonacciInput")
                .partitions(3)
                .replicas(1)
                //.compact()
                .build();
    }

    @Bean
    public NewTopic topicOutput() {
        return TopicBuilder.name("fibonacciOutput")
                .partitions(3)
                .replicas(1)
                .compact()
                .build();
    }

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kafkaStreamsConfiguration(){
        Map<String, Object> properties = new HashMap<>();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "fibonacci-application");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        return new KafkaStreamsConfiguration(properties);
    }

    @Bean
    public KStream<Long, Long> kStream(StreamsBuilder streamsBuilder){
        final int m = 5;

        KStream<Long, Long> stream = streamsBuilder.stream("fibonacciInput");
        stream
              .groupBy((key, value) -> key  / m)
              .aggregate(() -> 0L, (key, value, aggValue) -> aggValue + value)
                .toStream()
                .to("fibonacciOutput");
        return stream;
    }

    @Bean
    public ProducerFactory<Long, Long> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.ACKS_CONFIG, environment.getProperty("acks"));
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, environment.getProperty("bootstrap.servers"));
        configProps.put(ProducerConfig.RETRIES_CONFIG, environment.getProperty("retries"));
        configProps.put(ProducerConfig.BATCH_SIZE_CONFIG, environment.getProperty("batch.size"));
        configProps.put(ProducerConfig.LINGER_MS_CONFIG, environment.getProperty("linger.ms"));
        configProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, environment.getProperty("buffer.memory"));
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, environment.getProperty("key.serializer"));
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, environment.getProperty("value.serializer"));
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    KafkaTemplate<Long, Long> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    public ConsumerFactory<Long, Long> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, environment.getProperty("bootstrap.servers"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "foo");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<Long, Long>
    kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<Long, Long> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }

    @Bean
    StreamsBuilder streamsBuilder(){
        return new StreamsBuilder();
    }





}
