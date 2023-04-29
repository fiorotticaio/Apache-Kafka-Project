package kafka;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;
import org.springframework.kafka.support.serializer.JsonDeserializer;


public class CoffeeStockConsumer {
  public static void main(String[] args) {
    String BootstrapServers = "localhost:9092"; // Kafka server address
    String topic = "coffee_stock"; // Name of the topic to be consumed

    Properties prop = new Properties();
    prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BootstrapServers);
    prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    prop.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
    prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
    
    /* Consumer group settings */
    prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "stock_group");
    prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    prop.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    prop.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");

    KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(prop); // Create consumer

    consumer.subscribe(Arrays.asList(topic)); // Subscribe in topic "coffee_stock" 

    /* Our historical average */
    double closeAverage = 0.0, closeAverageAux = 0.0, closeAverageVariation = 0.0;
    int countSales = 0, recordCounts = 0;

    double coffeeValue = 4; // Initial value of coffee

    while (true) {
      /* Maximum waiting time for the message (in ms) */
      ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(1000));

      for (ConsumerRecord<String, byte[]> record : records) {
        byte[] valueBytes = record.value();
        JSONObject data = new JSONObject(new String(valueBytes, StandardCharsets.UTF_8)); // Convert byte[] to JSONObject

        JSONObject timeSeries = data.getJSONObject("Time Series (1min)"); // Object with all sales
        for (String key : timeSeries.keySet()) { // Go through all sales
          JSONObject timeSeriesObj = timeSeries.getJSONObject(key);
          Double close = Double.parseDouble(timeSeriesObj.getString("4. close")); // Get the close field
          
          if (recordCounts != 0) { // If it is not the first time, we have to compare if increase or decrease
            /* Manual change in close value */
            Random random = new Random();
            close = close + random.nextDouble()*10;
          } 

          closeAverageAux += close;
          countSales += 1;
        }

        /* Calculating the average of the last "countSales" sales */
        closeAverageAux /= countSales;

        /* Updating the historical average */
        /* If it is the first time, we have to set the historical average */
        if (recordCounts == 0) closeAverage = closeAverageAux;
        else {
          closeAverage = (closeAverage + closeAverageAux) / 2;
          closeAverageVariation = ((closeAverageAux - closeAverage) * 100) / closeAverage;
        }

        coffeeValue = changeCoffeeValue(coffeeValue, closeAverageVariation); // Change the coffee value
        sendCoffeeValueToTopic(coffeeValue); // Send the coffee value to the topic "coffee_price"
    
        /* Resetting aux variables */
        closeAverageAux = 0.0;
        countSales = 0;

        recordCounts++;
      }

      System.out.println("Close average: " + closeAverage);
      System.out.println("Coffee value: " + coffeeValue);
      System.out.print("\n");
    }
  }

  private static void sendCoffeeValueToTopic(double coffeeValue) {
    /* New producer that send a record in a specifc partition of the "coffe_price" topic */
    String BootstrapServers = "localhost:9092";
    String topic = "coffee_price";
    String partitionKey = "api_coffee_price";

    /* Setting procudor properties */
    Properties prop = new Properties();
    prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BootstrapServers);
    prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    KafkaProducer<String, String> producer = new KafkaProducer<>(prop); // Create the producer

    /* Create a record to a specific partitiof of the topic */
    String coffeeValueStr = Double.toString(coffeeValue);
    ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, partitionKey, coffeeValueStr);
    producer.send(record);
  }

  private static double changeCoffeeValue(double coffeeValue, double closeAverageVariation) {
    /* If the variation is greater than 1.5%, we increase the coffee value */
    if (closeAverageVariation >= 2 || closeAverageVariation <= 2) { 
      coffeeValue += closeAverageVariation / 10; // The change in price is the percentage increase/decrease / 10
    }
    return coffeeValue;
  }
}
