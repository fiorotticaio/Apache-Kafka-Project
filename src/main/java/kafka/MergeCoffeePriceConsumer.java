package kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

public class MergeCoffeePriceConsumer {
  public static void main(String[] args) {
    String BootstrapServers = "localhost:9092"; // Kafka server address
    String topic = "coffee_price"; // Name of the topic to be consumed
    int api_coffee_price = 2; // Partition to the key "api_coffee_price"
    int web_coffee_price = 0; // Partition to the key "web_coffee_price"
  
    Properties prop = new Properties();
    prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BootstrapServers);
    prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    prop.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
    prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    
    /* Consumer group settings */
    prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "price_group");
    prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    prop.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    prop.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");
  
    KafkaConsumer<String, String> consumerApiCoffeePricePartition = new KafkaConsumer<>(prop); // Create consumer 1
    KafkaConsumer<String, String> consumerWebCoffeePricePartition = new KafkaConsumer<>(prop); // Create consumer 2
  
    List<TopicPartition> partitions = Arrays.asList(new TopicPartition(topic, api_coffee_price));
    consumerApiCoffeePricePartition.assign((partitions)); // Subscribe in topic "coffee_price" in partition 2

    partitions = Arrays.asList(new TopicPartition(topic, web_coffee_price));
    consumerWebCoffeePricePartition.assign((partitions)); // Subscribe in topic "coffee_price" in partition 0

  
    while (true) {
      /* Maximum waiting time for the message (in ms) */
      ConsumerRecords<String, String> records1 = consumerApiCoffeePricePartition.poll(Duration.ofMillis(1000));
      System.out.println("Partition: " + api_coffee_price);
      for (ConsumerRecord<String, String> record : records1) {
        System.out.println("Offset: " + record.offset() + ", Key: " + record.key() + ", Value: " + record.value());
      }
      
      System.out.println("Partition: " + web_coffee_price);
      ConsumerRecords<String, String> records2 = consumerWebCoffeePricePartition.poll(Duration.ofMillis(1000));
      for (ConsumerRecord<String, String> record : records2) {
        System.out.println("Offset: " + record.offset() + ", Key: " + record.key() + ", Value: " + record.value());
      }
    }
  }
}
