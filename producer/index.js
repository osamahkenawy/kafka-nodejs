import Kafka from 'node-rdkafka';
import eventType from '../eventType.js';

const stream = Kafka.Producer.createWriteStream({
  'metadata.broker.list': 'localhost:9092'
}, {}, {
  topic: 'test'
});

stream.on('error', (err) => {
  console.error('Error in our kafka stream');
  console.error(err);
});

function queueRandomMessage() {
  const name = getRandomName();
  const language = getRandomLanguage();
  const experience = getRandomExperience();
  const timestamp = Date.now();
  const attributes = getRandomAttributes();
  const tags = getRandomTags();
  const event = { name, language, experience, timestamp, attributes, tags };
  const success = stream.write(eventType.toBuffer(event));     
  if (success) {
    console.log(`Message queued (${JSON.stringify(event)})`);
  } else {
    console.log('Too many messages in the queue already..');
  }
}

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

setInterval(() => {
  queueRandomMessage();
}, 3000);
