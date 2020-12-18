package by.epam.big_data.sevices;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.concurrent.CountDownLatch;

@Service
public class KafkaConsumer {
    private CountDownLatch latch = new CountDownLatch(3);

    @KafkaListener(topics = "fibonacci")
    public void listen(String message){
        System.out.println(message + "!!!!");
        latch.countDown();
    }


}
