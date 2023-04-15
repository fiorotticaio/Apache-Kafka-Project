package kafka;

import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Duration; 


public class Consumer {
  public static void main(String[] args) {
    String BootstrapServers = "localhost:9092";
    String topic = "sbux_stock";

    Logger logger = LoggerFactory.getLogger(Consumer.class.getName());

    Properties prop = new Properties();
    prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BootstrapServers);
    prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    
    // Configurações de grupos de consumidores
    prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-1");
    prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    prop.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    prop.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");

    KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(prop);

    // Assina o tópico "sbux_stock"
    consumer.subscribe(Arrays.asList(topic));

    while (true) {
      // TODO: mudar o consumidor pra ele receber JSON
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

      for (ConsumerRecord<String, String> record : records) {
        logger.info("Key: " + record.key() + ", Value:");
        logger.info("\n");
        logger.info(record.value());
        logger.info("\n\n");
      }
    }
  }
}
