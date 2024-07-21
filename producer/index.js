import Kafka from 'node-rdkafka';
import eventType from '../eventType.js';

// Custom partitioner function
function customPartitioner(numPartitions) {
  const partition = Math.floor(Math.random() * numPartitions);
  console.log(`Selected partition: ${partition}`);
  return partition;
}

// Create a Kafka Producer
const producer = new Kafka.Producer({
  'metadata.broker.list': 'localhost:9092',
});

producer.on('ready', () => {
  console.log('Producer is ready');

  function queueRandomMessage() {
    const name = getRandomName();
    const language = getRandomLanguage();
    const experience = getRandomExperience();
    const timestamp = Date.now();
    const attributes = getRandomAttributes();
    const tags = getRandomTags();
    const key = name; // Use name as the key for partitioning
    const event = { name, language, experience, timestamp, attributes, tags };
    const bufferedMessage = eventType.toBuffer(event);

    // Get the number of partitions for the topic
    producer.getMetadata({ topic: 'test', timeout: 10000 }, (err, metadata) => {
      if (err) {
        console.error('Error getting metadata:', err);
        return;
      }

      const numPartitions = metadata.topics[0].partitions.length;
      const partition = customPartitioner(numPartitions);
      producer.produce(
        'test', // Topic to send the message to
        partition, // Specific partition to use
        bufferedMessage, // Message content
        key, // Key for partitioning
        Date.now(), // Timestamp (optional)
        (err, offset) => {
          if (err) {
            console.error('Error producing message:', err);
          } else {
            console.log(`Produced message to partition ${partition} at offset ${offset}:`, event);
          }
        }
      );
    });
  }

  // Produce a message every 3 seconds
  setInterval(queueRandomMessage, 3000);
});

producer.on('event.error', (err) => {
  console.error('Error from producer:', err);
});

producer.connect();

// Helper functions to generate random data
function getRandomName() {
  const names = ['Alice', 'Bob', 'Charlie', 'David', 'Eve', 'Frank', 'Grace', 'Hannah', 'Ivy', 'Jack'];
  return names[Math.floor(Math.random() * names.length)];
}

function getRandomLanguage() {
  const languages = ['JAVASCRIPT', 'PYTHON', 'JAVA', 'CPP', 'RUBY'];
  return languages[Math.floor(Math.random() * languages.length)];
}

function getRandomExperience() {
  const experiences = ['1 year', '2 years', '3 years', '4 years', '5+ years'];
  return experiences[Math.floor(Math.random() * experiences.length)];
}

function getRandomAttributes() {
  const levels = ['JUNIOR', 'MID', 'SENIOR'];
  const locations = ['New York', 'San Francisco', 'Berlin', 'Tokyo', 'Sydney'];
  return {
    level: levels[Math.floor(Math.random() * levels.length)],
    location: locations[Math.floor(Math.random() * locations.length)]
  };
}

function getRandomTags() {
  const tags = ['frontend', 'backend', 'fullstack', 'devops', 'data', 'ai'];
  const randomTags = [];
  const numTags = Math.floor(Math.random() * tags.length) + 1;
  for (let i = 0; i < numTags; i++) {
    randomTags.push(tags[Math.floor(Math.random() * tags.length)]);
  }
  return randomTags;
}
