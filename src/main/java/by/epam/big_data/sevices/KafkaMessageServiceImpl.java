package by.epam.big_data.sevices;

import by.epam.big_data.sevices.KafkaMessageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaMessageServiceImpl implements KafkaMessageService {


    //@Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public KafkaMessageServiceImpl(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void sendMsg(String msg){
        /*ListenableFuture<SendResult<String, String>> future = */

        //LOGGER.info("sending message='{}' to topic='{}'", msg, "fibonacci" );
        kafkaTemplate.send("fibonacci", msg);

//        future.addCallback(System.out::println,
//                System.err::println);

        //kafkaTemplate.flush();
    }

}
