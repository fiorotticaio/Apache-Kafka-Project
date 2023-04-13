package kafka;

import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;


public class ConsumerWithGroupProps {
  public static void main(String[] args) {
    String BootstrapServers = "localhost:9092";
    String Topic = "testTopic";

    Logger logger = LoggerFactory.getLogger(Consumer.class.getName());

    Properties prop = new Properties();
    prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BootstrapServers);
    prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    
    // Configurações de grupos de consumidores
    prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-1");
    prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    prop.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    prop.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");

    KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(prop);

    consumer.subscribe(Arrays.asList(Topic));

    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
      for (ConsumerRecord<String, String> record : records) {
        logger.info("Key: " + record.key() + ", Value: " + record.value());
        logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
        logger.info("\n");
      }
      consumer.commitSync(); // Commitar offsets do grupo
    }
  }
}
