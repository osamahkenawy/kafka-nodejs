import Kafka from 'node-rdkafka';
import eventType from '../eventType.js';

const consumer = new Kafka.KafkaConsumer({
  'group.id': 'kafka',
  'metadata.broker.list': 'localhost:9092',
}, {});

consumer.connect();

consumer.on('ready', () => {
  console.log('Consumer is ready');
  consumer.subscribe(['test']);
  consumer.consume();
}).on('data', (data) => {
  try {
    const event = eventType.fromBuffer(data.value);
    console.log('Received message:', event);
  } catch (err) {
    console.error('Failed to decode message:', err);
    console.error('Raw message data:', data.value);
  }
});

consumer.on('event.error', (err) => {
  console.error('Error from consumer:', err);
});
