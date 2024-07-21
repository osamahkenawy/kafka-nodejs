console.log("Producer")
import kafka from 'node-rdkafka';
const stream = kafka.Producer.createWriteStream({ "metadata.broker.list": 'localhost:9092'},
     {},
      {
        topic: 'test'
      }
);
function queueRandomMessage() {
    const success = stream.write(Buffer.from('hi osamah'));     
    console.log(success)
     if (success) {
      console.log(`message queued Succesfully`);
    } else {
      console.log('Too many messages in the queue already..');
    }
  }
setInterval(() => {
    queueRandomMessage();
  }, 3000);