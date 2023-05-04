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
    
    /* Kafka configuration */
    String BootstrapServer = "localhost:9092"; 
    String sourceTopic = "coffee_stock"; 
    String destinationTopic = "coffee_price";
    int inflationFactor = 5;

    /* Setting consumer properties */
    Properties prop = new Properties();
    prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BootstrapServer);
    prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    prop.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
    prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
    
    /* Consumer group settings (this is not necessary. Learning purposes)*/
    prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "stock_group");
    prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    prop.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    prop.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");

    /* Creating consumer and subscribing to sourceTopic */
    KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(prop); 
    consumer.subscribe(Arrays.asList(sourceTopic)); 

    /* Initialization of arbitrary value for coffee */
    Double globalCloseAverage = 106.0, coffeeValue = 4.0;

    while (true) {
      /* Polling from topic each second */
      ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(3000));

      /* Iterating over all the requests stored on kafka and converting the byte[] to JSON */
      for (ConsumerRecord<String, byte[]> record : records) {
        Double sumCloseValue = 0.0;
        int countSales=0;

        byte[] valueBytes = record.value();
        JSONObject data = new JSONObject(new String(valueBytes, StandardCharsets.UTF_8)); 
        JSONObject timeSeries = data.getJSONObject("Time Series (1min)"); 

        /* Iterate over sales on a single request */
        for (String key : timeSeries.keySet()) { 
          /* Recieving the 'close' value */
          JSONObject timeSeriesObj = timeSeries.getJSONObject(key);
          Double close = Double.parseDouble(timeSeriesObj.getString("4. close")); 
          
          sumCloseValue += close;
          countSales += 1;
        }

        /* Calculating the average for these sales*/
        Double closeAverageOnRequest = sumCloseValue/countSales;

        /* Inserting this influence on global close average */
        globalCloseAverage=(globalCloseAverage+closeAverageOnRequest)/2;
      }

      /* Here, for illustration purposes, the 'close' value receives a random change */
      int increaseOrDecrease = 0;
      if (new Random().nextDouble()>.5) increaseOrDecrease=-1; else increaseOrDecrease=1;
      Double variation = (new Random().nextDouble()*inflationFactor)*increaseOrDecrease;
      Double randomClose = globalCloseAverage + variation;

      /* Updating the historical average or setting new one if it is the first iteration */
      Double lastAverage = globalCloseAverage;
      globalCloseAverage=(globalCloseAverage+randomClose)/2;
      Double closeAverageVariation = ((globalCloseAverage - lastAverage) / globalCloseAverage) * 100;
    
      /* Updating coffee value */
      coffeeValue = changeCoffeeValue(coffeeValue, closeAverageVariation); 
      
      /* Sending coffee value to the 'coffee_price' topic */
      sendCoffeeValueToTopic(0,destinationTopic, BootstrapServer, coffeeValue); 

      System.out.println("Publishing Close average ("+globalCloseAverage+") on api partition at api_coffee_price");
      System.out.println("Publishing Coffee value ("+coffeeValue+") on api partition at api_coffee_price");
      System.out.print("\n");
    }
  }

  private static void sendCoffeeValueToTopic(int id, String topic, String BootstrapServer, double coffeeValue) {
    /*TODO: 
     * rever se precisa realmente ficar criando sempre esse producer aqui
     * ele sendo criado toda hora gera mta sujeira no console e fica dificil de debugar
     * talvez seja bom colocar ele do lado de fora e só chamar aqui o .send()
    */
    
    /* Setting partition identification */
    int api_coffee_price = 0;
    
    /* Setting new producer properties */
    Properties prop = new Properties();
    prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BootstrapServer);
    prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    /* Instantiating new producer */
    KafkaProducer<String, String> producer = new KafkaProducer<>(prop);

    /* Creating a record on the partition, with the new coffee value */
    String coffeeValueStr = Double.toString(coffeeValue);
    ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, api_coffee_price, "id_"+id, coffeeValueStr);
    producer.send(record);
    producer.close();
    
    System.out.println("Publishing Coffee value ("+coffeeValue+") at api_coffee_price");
  }

  private static double changeCoffeeValue(double coffeeValue, double closeAverageVariation) {
    /* If the variation is greater than 1.5%, the coffee value increases */
    if (closeAverageVariation >= 2 || closeAverageVariation <= 2) { 
      /* The increase value is 'boosted' for generating more visual effect */
      coffeeValue += closeAverageVariation / 10; 
    }
    return coffeeValue;
  }
}
