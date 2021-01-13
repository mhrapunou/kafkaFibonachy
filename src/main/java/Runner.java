import by.epam.big_data.config.SpringConfig;
import by.epam.big_data.sevices.KafkaConsumer;
import by.epam.big_data.sevices.KafkaProducerService;
import by.epam.big_data.sevices.KafkaProducerServiceImpl;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.kafka.annotation.EnableKafka;

import java.util.ArrayList;
import java.util.List;


public class Runner {

    public static void main(String[] args) {
        int n = 20;
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(SpringConfig.class);
        KafkaProducerService kafkaProducerService = context.getBean("kafkaProducerServiceImpl", KafkaProducerServiceImpl.class);
        List<Long> fibonacciConsequence = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            if (i < 2) {
                fibonacciConsequence.add((long) i);
            } else {
                long fibonacciNumber = fibonacciConsequence.get(i - 1) + fibonacciConsequence.get(i - 2);
                fibonacciConsequence.add(fibonacciNumber);
            }
            kafkaProducerService.sendMsg((long) i, fibonacciConsequence.get(i));
        }
        System.out.println("done");

    }
}
