package kafka;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.support.serializer.JsonSerializer;


public class ApiProducer {
  public static void main(String[] args) throws InterruptedException, IOException {
    String BootstrapServers = "localhost:9092";
    String topic = "coffee-stock";

    /* Setting procudor properties */
    Properties prop = new Properties();
    prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BootstrapServers);
    prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());

    KafkaProducer<String, byte[]> producer = new KafkaProducer<>(prop); // create the producer

    /* Connection with Alpha Vantage API */
    String function = "TIME_SERIES_INTRADAY";
    String apiKey = "0WCXGJ9X5SRNOH5M";
    String symbol = "SBUX";
    String interval = "1min";
    String url = String.format("https://www.alphavantage.co/query?function=%s&interval=%s&symbol=%s&apikey=%s", function, interval, symbol, apiKey);

    HttpClient client = HttpClientBuilder.create().build();
    int i = 0;
    while (true) {
      i+=1;
      HttpGet request = new HttpGet(url);
      URL apiUrl = new URL(url);
      HttpURLConnection conn = (HttpURLConnection) apiUrl.openConnection();
      conn.setRequestMethod("GET");

      try {
        HttpResponse response = client.execute(request);
        String responseObject = EntityUtils.toString(response.getEntity());
        byte[] value = responseObject.getBytes(StandardCharsets.UTF_8);
        ProducerRecord<String, byte[]> record = new ProducerRecord<String, byte[]>(topic, "id_"+i, value);

        // TODO: definir uma key e um value para esse evento
        producer.send(record);
      } catch (Exception ex) {
        ex.printStackTrace();
      }

      Thread.sleep(20000); // wait 1min before get new data
    }
  }
}