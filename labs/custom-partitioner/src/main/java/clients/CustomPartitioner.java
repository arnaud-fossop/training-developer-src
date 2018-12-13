package clients;

import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.serialization.StringSerializer;

public class CustomPartitioner implements Partitioner {
    public static void main(String[] args) {
        System.out.println("*** Starting Basic Producer ***");

        // here will be the Kafka Producer code
        Properties settings = new Properties();
        settings.put("client.id", "basic-producer");
        settings.put("bootstrap.servers", "kafka:9092");
        settings.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        settings.put("value.serializer", StringSerializer.class);
        settings.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class);

        final KafkaProducer<String, String> producer = new KafkaProducer<>(settings);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("### Stopping Basic Producer ###");
            producer.close();
        }));

        final String topic = "custom-partitioner-topic";
        for(int i=1; i<=20; i++) {
            final String value = new Integer(new Random().nextInt(19) + 1).toString();
            final String key = "key-" + value;
            final ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            producer.send(record);
        }
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        Integer intValue = Integer.parseInt((String)value);
        int result = (intValue != null && intValue.intValue() <= 10) ? 0 : 1;
        System.out.printf("strvalue: %s, int value: %d, result: %d\n", value, intValue, result);
        return result;
    }

    @Override
	public void close() {
		
	}
}