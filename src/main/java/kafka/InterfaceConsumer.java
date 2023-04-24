package kafka;

import java.util.Arrays;
import java.util.Properties;
import java.time.Duration;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.springframework.kafka.support.serializer.JsonDeserializer; 

public class InterfaceConsumer {
    public static void main(String[] args) {
        String bootstrapServers = "localhost:9092"; // Kafka server address
        String topic = "coffee-sales"; // Name of the topic to be consumed

        /* Consumer settings */
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        /* Consumer group settings */
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "coffe-sales-consumer-group");
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        prop.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        prop.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");

        Consumer<String, String> consumer = new KafkaConsumer<>(prop); // Create consumer

        consumer.subscribe(Arrays.asList(topic)); // Subscribe in topic "sbux-sale" 

        int countCoffeeSales = 0;

        /* Loop to consume messages */
        while (true) {
            /* Maximum waiting time for the message (in ms) */
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000)); 
            
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("Received message: " + record.value());
                // int coffeeValue = Integer.parseInt(record.value()); // TODO: receber o valor do café lá na interface
                countCoffeeSales++;
            }

            if (countCoffeeSales >= 20) { // So many coffee sales
                /* Increase the coffee price */
                System.out.println("Aumentou o preço do café");
                countCoffeeSales = 0; // Reset the counter
            }
        }
    }
}