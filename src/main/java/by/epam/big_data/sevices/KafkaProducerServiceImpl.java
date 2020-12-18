package by.epam.big_data.sevices;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@Service
public class KafkaProducerServiceImpl implements KafkaProducerService {


    @Autowired
    private KafkaTemplate<Long, Long> kafkaTemplate;

    @Override
    public void sendMsg(Long msg){

        System.out.println(msg);
        ProducerRecord<Long, Long> record = new ProducerRecord<>("fibonacci", msg);

        //ListenableFuture<SendResult<String, String>> future =
        kafkaTemplate.send(record);


       // future.addCallback(LOGGER::info, LOGGER::error);

       // kafkaTemplate.flush();
    }

}
