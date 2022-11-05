import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;

public class WorkshopConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();

        props.put("bootstrap.servers", "104.131.12.157:9093");


        props.put("group.id", "testgroup");
        props.put("auto.offset.reset", "latest");
        props.put("auto.commit.interval.ms", "1000");


        props.put("key.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<Long, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("test-topic"));

        final KafkaProducer<Long, String> producer = new KafkaProducer<>(props);

        int batchSize = 200;

        List<ConsumerRecord<Long, String>> batch = new ArrayList<>();
        while (true) {
            ConsumerRecords<Long, String> records = consumer.poll(100);
            System.out.println(record.value);
        }
        long id = 0;

        while (true) {
            try {
                Thread.sleep(1000);
                ProducerRecord<Long, String> message = new ProducerRecord<>("test-topic", id, "cj #" + id);

                producer.send(message);
                id++;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
