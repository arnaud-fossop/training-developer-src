package clients;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import solution.model.ShakespeareKey;
import solution.model.ShakespeareValue;

public class ShakespeareAvroConsumer {

    private static String WORK = "Hamlet";
    private static int YEAR = 1600;

    public static void main(String[] args) {
        System.out.println("Hello World");
        System.out.printf("Reading of the book %s, %d\n", WORK, YEAR);
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        ShakespeareAvroConsumer consumer = new ShakespeareAvroConsumer();
        consumer.readBook(WORK, YEAR);
    }

    private KafkaConsumer<ShakespeareKey,ShakespeareValue> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "other_group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put("schema.registry.url", "http://schema-registry:8081");

        KafkaConsumer<ShakespeareKey,ShakespeareValue> consumer = new KafkaConsumer<>(props);
        return consumer;
    }

    public void readBook(String work, int year) {
        try(KafkaConsumer<ShakespeareKey, ShakespeareValue> consumer = createConsumer()) {
            consumer.subscribe(Arrays.asList("shakespeare_avro_topic"));
            consumer.poll(0);
            consumer.seekToBeginning(consumer.assignment());

            while(true) {
                ConsumerRecords<ShakespeareKey, ShakespeareValue> records = consumer.poll(Duration.ofMillis(100));

                records.forEach(record -> {
                    ShakespeareKey key = record.key();
                    ShakespeareValue value = record.value();

                    if (key.getWork().equalsIgnoreCase(work) && key.getYear().intValue() == year) {
                        System.out.println(value.getLineNumber() + " " + value.getLine());
                    }
                });
                
            }
        }
        
    }
}