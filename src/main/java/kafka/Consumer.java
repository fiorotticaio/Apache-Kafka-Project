package kafka;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer; 


public class Consumer {
  public static void main(String[] args) {
    String BootstrapServers = "localhost:9092";
    String topic = "sbux_stock";

    Logger logger = LoggerFactory.getLogger(Consumer.class.getName());

    Properties prop = new Properties();
    prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BootstrapServers);
    prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    prop.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
    prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
    
    // Configurações de grupos de consumidores
    prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-1");
    prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    prop.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    prop.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");

    KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(prop);

    // Assina o tópico "sbux_stock"
    consumer.subscribe(Arrays.asList(topic));

    while (true) {
      ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(1000));

      for (ConsumerRecord<String, byte[]> record : records) {
        logger.info("Key: " + record.key() + ", Value:\n");
        byte[] valueBytes = record.value();
        JSONObject data = new JSONObject(new String(valueBytes, StandardCharsets.UTF_8));

        /* Testing to read specific atributes of the data */
        JSONObject allSales = data.getJSONObject("Time Series (1min)");
        JSONObject specificSale = allSales.getJSONObject("2023-04-14 15:17:00");
        String high = specificSale.getString("2. high");
        logger.info("High: " + high);

        // logger.info(data.toString());
        // logger.info("\n\n");
      }
    }
  }
}
