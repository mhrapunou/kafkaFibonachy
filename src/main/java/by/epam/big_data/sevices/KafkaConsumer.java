package by.epam.big_data.sevices;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;

@EnableKafka
@Service
public class KafkaConsumer {
    private CountDownLatch latch = new CountDownLatch(3);
    private int m = 5;
    private long minOutputIndex = 0;
    private long maxOutputIndex = m;
    private NavigableMap<Long, Long> records = new TreeMap<>();

    @KafkaListener(topics = "fibonacciOutput")
    public void listen(ConsumerRecord<Long, Long> record){

        System.out.println("p= " + record.partition());
        System.out.print(" K= " + record.key());
        System.out.println(" v= " + record.value());
//        records.put(record.key(), record.value());
//        NavigableMap<Long, Long> mapForOutput = records.subMap(minOutputIndex, true, maxOutputIndex, false);
//        if (mapForOutput.size() == m){
//            System.out.println(mapForOutput.values().stream().reduce(0L, Long::sum));
//            minOutputIndex += m;
//            maxOutputIndex += m;
//            System.out.println(mapForOutput);
//        }
//        latch.countDown();
    }


}
