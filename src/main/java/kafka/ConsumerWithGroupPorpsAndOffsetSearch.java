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
import java.time.Duration;


public class ConsumerWithGroupPorpsAndOffsetSearch {
  public static void main(String[] args) {
    String BootstrapServers = "localhost:9092";
    String Topic = "testTopic";

    Logger logger = LoggerFactory.getLogger(Consumer.class.getName());

    Properties prop = new Properties();
    prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BootstrapServers);
    prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    
    // Configurações de grupos de consumidores
    prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-1");
    prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    prop.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    prop.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");

    KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(prop);

    // Variável para o offset desejado
    long offset = 30L; // L: indicar que é do tipo long

    // Chamar o método seek() para ir para um offset específico
    TopicPartition partition = new TopicPartition(Topic, 0); // 0 é a partição que desejamos buscar
    consumer.assign(Arrays.asList(partition));
    consumer.seek(partition, offset);

    // Consumir registros a partir do offset específico
    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
      for (ConsumerRecord<String, String> record : records) {
        // Verifique se estamos na partição correta e no offset desejado
        if (record.partition() == partition.partition() && record.offset() == offset) {
          // Imprimindo o registro encontrado
          logger.info("Found record at partition " + record.partition() + ", offset " + record.offset() + 
          ", key=" + record.key() + ", value=" + record.value());
          return; // Encerrando o loop depois de encontrar o registro desejado
        }
      }
    }
  }
}
