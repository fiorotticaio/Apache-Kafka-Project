package kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

public class MergeCoffeePriceConsumer {
  public static void main(String[] args) {
    /* Kafka configuration */
    String BootstrapServer = "localhost:9092"; 
    String topic = "coffee_price"; 

    /* Setting partitions on topic (one for api response value, and other for interaction on UI) */
    int api_coffee_price = 0;
    int web_coffee_price = 1;
    int real_coffee_price = 2;
  
    /* Setting consumer properties */
    Properties prop = new Properties();
    prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BootstrapServer);
    prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    prop.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
    prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    
    /* Consumer group settings (this is not necessary. Learning purposes)*/
    prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "price_group");
    prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    prop.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    prop.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");
  
    /* Creating two consumers, one for each partition */  
    KafkaConsumer<String, String> consumerApiCoffeePricePartition = new KafkaConsumer<>(prop);
    KafkaConsumer<String, String> consumerWebCoffeePricePartition = new KafkaConsumer<>(prop);
    
    /* Assining the api partition on the api consumer */
    TopicPartition apiPartition = new TopicPartition(topic, api_coffee_price);
    consumerApiCoffeePricePartition.assign(Collections.singletonList(apiPartition));
    
    // /* Assining the web partition on the web consumer */
    // TopicPartition webPartition = new TopicPartition(topic, web_coffee_price);
    // consumerWebCoffeePricePartition.assign(Collections.singletonList(webPartition));

    String apiCoffePriceStr = "0.0";
    String webCoffePriceStr = "0.0";
    int count=0;
    
    while (true) {
      /* Polling from api partition on topic, each second */
      ConsumerRecords<String, String> records2 = consumerApiCoffeePricePartition.poll(Duration.ofMillis(2000));
      for (ConsumerRecord<String, String> record : records2) {
        /* Storing the value for api partition on string */
        apiCoffePriceStr = record.value();
      }

      // /* Polling from web partition on topic, each second */
      // ConsumerRecords<String, String> records1 = consumerWebCoffeePricePartition.poll(Duration.ofMillis(2000));
      // for (ConsumerRecord<String, String> record : records1) {
      //   /* Storing the value for web partition on string */
      //   webCoffePriceStr = record.value();
      // }

      /* Merging both values */
      double realCoffeePrice = mergePrices(Double.parseDouble(webCoffePriceStr), Double.parseDouble(apiCoffePriceStr));
      System.out.printf("Real coffee price [%s/%s]: %.2f\n", webCoffePriceStr, apiCoffePriceStr, realCoffeePrice);

      sendRealCoffeePriceToTopic(count++, topic, real_coffee_price, BootstrapServer, realCoffeePrice);
    }
  }

  private static void sendRealCoffeePriceToTopic(int id, String topic, int partition, String BootstrapServer, double realCoffeePrice) {
    /* New producer that send a record in the last partition of the "coffee_price" topic */
    Properties prop = new Properties();
    prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BootstrapServer);
    prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    /* Creating producer to send the merged value to partition */
    KafkaProducer<String, String> producer = new KafkaProducer<>(prop);

    /* Create a record to partition of the topic */
    String realCoffeePriceStr = Double.toString(realCoffeePrice);
    ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, partition, "id_"+id, realCoffeePriceStr);
    producer.send(record);
    producer.close();
  }

  private static double mergePrices(double webPrice, double apiPrice) {
    /* Checking if one of the prices are not set, if so, set the other */
    if (webPrice==0 && apiPrice!=0) return apiPrice;
    else if (webPrice!=0 && apiPrice==0) return webPrice;
    
    /* The merging strategy is a simple arithmetic average */
    return (webPrice + apiPrice) / 2;
  }
}
