package kafka;

import java.util.Arrays;
import java.util.Properties;
import java.time.Duration;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;


public class InterfaceConsumer {
    public static void main(String[] args) {
        
        /* Kafka configuration */
        String BootstrapServer = "localhost:9092"; 
        String sourceTopic = "coffee_sales"; 
        String destinationTopic = "coffee_price";
        int web_coffee_price = 1;

        /* Consumer settings */
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BootstrapServer);
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        /* Consumer group settings */
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "coffee-sales-consumer-group");
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        prop.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        prop.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");

        /* Creating consumer to receive the records from UI calls */
        Consumer<String, String> consumer = new KafkaConsumer<>(prop); 
        consumer.subscribe(Arrays.asList(sourceTopic)); 

        /* Initialization of the parameter that controls the amount of sales registered */
        int countCoffeeSales = 0;

        /* Loop to consume messages */
        while (true) {
            /* Polling from web partition, each second */
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000)); 
            
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("Received message: " + record.value());
                // int coffeeValue = Integer.parseInt(record.value()); // TODO: receber o valor do café lá na interface
                countCoffeeSales++;

                System.out.println("COUNT: " + countCoffeeSales);
                
            }

            /* If sales surpass some threshold, the overall price of coffee rises */
            if (countCoffeeSales >= 20) { 
                Double newCoffeeValue = 5.0;    
                
                //...

                sendSalesToTopic(destinationTopic, web_coffee_price, BootstrapServer, newCoffeeValue);
                System.out.println("Aumentou o preço do café");
                countCoffeeSales = 0; 
            }
        }
    }


    private static void sendSalesToTopic(String topic, int partition, String BootstrapServer, Double newCoffeeValue){
        /* Setting new producer properties */
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BootstrapServer);
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        /* Instantiating new producer */
        KafkaProducer<String, String> producer = new KafkaProducer<>(prop);

        /* Creating a record on the partition, with the new coffee value */
        String coffeeValueStr = Double.toString(newCoffeeValue);
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, partition, "UI_change", coffeeValueStr);
        producer.send(record);
        producer.close();
    }
}

