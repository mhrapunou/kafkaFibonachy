package by.epam.big_data.sevices;

public interface KafkaProducerService {
    public void sendMsg(Long key, Long msg);

}
