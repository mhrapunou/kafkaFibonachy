package by.epam.big_data.config;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.*;

import java.util.HashMap;
import java.util.Map;

@Configuration
@ComponentScan("by.epam.big_data")
@PropertySource("classpath:producerConfig.properties")
@EnableKafka
public class SpringConfig {

    @Autowired
    Environment environment;

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
    KafkaTemplate<Long, Long> kafkaTemplate(){
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, environment.getProperty("bootstrap.servers"));
        //props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(props);
    }
}
