package kafka;


import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Producer {
  public static void main(String[] args) throws InterruptedException, IOException {
    String BootstrapServers = "localhost:9092";
    String topic = "sbux_stock";
    Logger logger = LoggerFactory.getLogger(Producer.class);

    /* Setting procudor properties */
    Properties prop = new Properties();
    prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BootstrapServers);
    prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());

    KafkaProducer<String, JSONObject> producer = new KafkaProducer<>(prop); // create the producer

    /* Connection with Alpha Vantage API */
    String function = "TIME_SERIES_INTRADAY";
    String apiKey = "0WCXGJ9X5SRNOH5M";
    String symbol = "SBUX";
    String interval = "1min";
    String url = String.format("https://www.alphavantage.co/query?function=%s&interval=%s&symbol=%s&apikey=%s", function, interval, symbol, apiKey);

    HttpClient client = HttpClientBuilder.create().build();

    while (true) {
      HttpGet request = new HttpGet(url);
      URL apiUrl = new URL(url);
      HttpURLConnection conn = (HttpURLConnection) apiUrl.openConnection();
      conn.setRequestMethod("GET");
      JSONObject data = null;

      try {
        HttpResponse response = client.execute(request);
        String json = EntityUtils.toString(response.getEntity());
        data = new JSONObject(json);
      } catch (Exception ex) {
        ex.printStackTrace();
      }

      System.out.println("JSON GERADO: " + data);

      
      ProducerRecord<String, JSONObject> record = new ProducerRecord<>(topic, data);
      producer.send(record);
      Thread.sleep(60000); // espera 1min segundos antes de buscar novos dados
    }
  }
}