import by.epam.big_data.config.SpringConfig;
import by.epam.big_data.sevices.KafkaMessageService;
import by.epam.big_data.sevices.KafkaMessageServiceImpl;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.kafka.annotation.EnableKafka;


@EnableKafka
public class Runner {

//    @KafkaListener(topics = "fibonacci")
//    public void msgListener(ConsumerRecord<String, String> record){
//        System.out.println(record.partition());
//        System.out.println(record.key());
//        System.out.println(record.value());
//    }

    public static void main(String[] args) {
        String msg = "Preved!";
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(SpringConfig.class);
        KafkaMessageService kafkaMessageService = context.getBean("kafkaMessageServiceImpl", KafkaMessageServiceImpl.class);

        kafkaMessageService.sendMsg(msg);
    }
}
