const { Kafka } = require('kafkajs')
 
const kafka = new Kafka({
  clientId: 'my-app',
  brokers: [
      'aafefc11749eb11eaa601026ac274977-1293374973.us-east-1.elb.amazonaws.com:9092'
    ]
})
console.log('Kafka server connected!')

const producer = kafka.producer()
const consumer = kafka.consumer({ groupId: 'test-group' })
console.log('Producer and consumer ready!')
 
const run = async () => {
  // Producing
  console.log('Connecting on producer...')
  await producer.connect()
  console.log('Producer connected!')
  console.log('Sending message...')
  await producer.send({
    topic: 'audsatplatform-teste-topics',
    messages: [
      { value: 'Hello KafkaJS user!' },
    ],
  })
  console.log('Message sent...')
 
  // Consuming
  await consumer.connect()
  console.log('Consumer connected!')
  await consumer.subscribe({ topic: 'audsatplatform-teste-topics', fromBeginning: true })
  console.log('Listening for topic...')
 
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        partition,
        offset: message.offset,
        value: message.value.toString(),
      })
    },
  })
}

console.log('Done!')
run().catch(console.error)