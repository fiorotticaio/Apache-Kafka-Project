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


public class ApiConsumer {
  public static void main(String[] args) {
    String BootstrapServers = "localhost:9092";
    String topic = "sbux_stock";

    Logger logger = LoggerFactory.getLogger(ApiConsumer.class.getName());

    Properties prop = new Properties();
    prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BootstrapServers);
    prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    prop.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
    prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
    
    /* Consumer group settings */
    prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-1");
    prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    prop.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    prop.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");

    KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(prop);

    /* Subscribe in topic "sbux_stock" */
    consumer.subscribe(Arrays.asList(topic));

    /* Our historical average */
    double openAverage = 0.0, openAverageAux = 0.0;
    double highAverage = 0.0, highAverageAux = 0.0;
    double lowAverage = 0.0, lowAverageAux = 0.0;
    double closeAverage = 0.0, closeAverageAux = 0.0;
    double volumeAverage = 0.0, volumeAverageAux = 0.0;
    int countSales = 0, recordCounts = 0;

    int maior = 0, menor = 0, igual = 0;

    while (true) {
      ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(1000));

      for (ConsumerRecord<String, byte[]> record : records) {
        logger.info("Key: " + record.key() + ", Value:\n");
        byte[] valueBytes = record.value();
        JSONObject data = new JSONObject(new String(valueBytes, StandardCharsets.UTF_8));

        System.out.println(data);

        JSONObject timeSeries = data.getJSONObject("Time Series (1min)"); // Object with all seals
        for (String key : timeSeries.keySet()) { // Go through all sales
          JSONObject timeSeriesObj = timeSeries.getJSONObject(key);
          Double open = Double.parseDouble(timeSeriesObj.getString("1. open")); // Get the open field
          Double high = Double.parseDouble(timeSeriesObj.getString("2. high")); // Get the high field
          Double low = Double.parseDouble(timeSeriesObj.getString("3. low")); // Get the low field
          Double close = Double.parseDouble(timeSeriesObj.getString("4. close")); // Get the close field
          Double volume = Double.parseDouble(timeSeriesObj.getString("5. volume")); // Get the volume field

          if (recordCounts != 0) { // If it is not the first time, we have to compare if increase or decrease
            if (high > highAverage) maior++;
            else if (high < highAverage) menor++;
            else igual++;
          } 

          openAverageAux += open;
          highAverageAux += high;
          lowAverageAux += low;
          closeAverageAux += close;
          volumeAverageAux += volume;

          countSales += 1;
        }

        /* Calculating the average of the last "countSales" sales */
        openAverageAux /= countSales;
        highAverageAux /= countSales;
        lowAverageAux /= countSales;
        closeAverageAux /= countSales;
        volumeAverageAux /= countSales;

        /* Updating the historical average */
        if (recordCounts == 0) { // If it is the first time, we have to set the historical average
          openAverage = openAverageAux;
          highAverage = highAverageAux;
          lowAverage = lowAverageAux;
          closeAverage = closeAverageAux;
          volumeAverage = volumeAverageAux;
        } else {
          openAverage = (openAverage + openAverageAux) / 2;
          highAverage = (highAverage + highAverageAux) / 2;
          lowAverage = (lowAverage + lowAverageAux) / 2;
          closeAverage = (closeAverage + closeAverageAux) / 2;
          volumeAverage = (volumeAverage + volumeAverageAux) / 2;

          System.out.println("Maior: " + maior);
          System.out.println("Menor: " + menor);
          System.out.println("Igual: " + igual);
        }

        /* Resetting aux variables */
        openAverageAux = 0.0;
        highAverageAux = 0.0;
        lowAverageAux = 0.0;
        closeAverageAux = 0.0;
        volumeAverageAux = 0.0;
        countSales = 0;

        recordCounts++;

        maior = 0;
        menor = 0;
        igual = 0;
      }

      // System.out.println("Open average: " + openAverage);
      // System.out.println("High average: " + highAverage);
      // System.out.println("Low average: " + lowAverage);
      // System.out.println("Close average: " + closeAverage);
      // System.out.println("Volume average: " + volumeAverage);
      // System.out.print("\n");
    }
  }
}
