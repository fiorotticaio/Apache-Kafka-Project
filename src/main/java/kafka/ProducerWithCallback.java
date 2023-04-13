package kafka;

import java.util.Properties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ProducerWithCallback {
  public static void main(String[] args) {
    String BootstrapServers = "localhost:9092";
    String Topic = "testTopic";

    Logger logger = LoggerFactory.getLogger(ProducerWithCallback.class);

    Properties prop = new Properties();
    prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BootstrapServers);
    prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);

    ProducerRecord<String, String> record = new ProducerRecord<String, String>(Topic, "Hello, Kafka!");

    producer.send(record, new Callback() {
      @Override
      public void onCompletion(RecordMetadata metadata, Exception e) {
        if (e == null) {
          logger.info("Received new metadata. \n" +
            "Topic: " + metadata.topic() + "\n" +
            "Partition: " + metadata.partition() + "\n" +
            "Offset: " + metadata.offset() + "\n" +
            "Timestamp: " + metadata.timestamp());
        } else {
          logger.error("Error while producing", e);
        }
      }
    });
    
    producer.flush(); // Atualiza o producer
  }
}