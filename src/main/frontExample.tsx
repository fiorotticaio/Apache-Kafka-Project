// Run npm install kafkajs first
import { Kafka } from 'kafkajs';

/* Kafka configs */
const kafka = new Kafka({
  clientId: 'coffe-producer',
  brokers: ['localhost:9092']
});

const producer = kafka.producer(); // Create the producer

/* Function that sends messages to kafka */
async function sendCoffeeSale(value: number) {
  await producer.connect();
  await producer.send({
    topic: 'sbux-sale',
    messages: [{ value: value.toString() }]
  });
  await producer.disconnect();
}

/* Example call to send coffee sale to Kafka */
const coffeeValue = 2.50; // Value of coffee sale
sendCoffeeSale(coffeeValue);