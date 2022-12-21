const { Kafka, CompressionTypes, logLevel } = require("kafkajs");

const host = "";

const kafka = new Kafka({
  logLevel: logLevel.DEBUG,
  brokers: [`${host}:9092`],
  clientId: "example-producer",
});

const topic = "y-doc-topic";
const producer = kafka.producer();

const sendMessage = (msg) => {
  return producer
    .send({
      topic,
      compression: CompressionTypes.GZIP,
      messages: {key: msg.chapter_id, value: msg},
    })
    .then(console.log)
    .catch((e) => console.error(`[example/producer] ${e.message}`, e));
};

exports.run = async (msg) => {
  console.log("producer");

  await producer.connect();
  sendMessage(msg);
};

// exports.run().catch(e => console.error(`[example/producer] ${e.message}`, e))

const errorTypes = ["unhandledRejection", "uncaughtException"];
const signalTraps = ["SIGTERM", "SIGINT", "SIGUSR2"];

errorTypes.forEach((type) => {
  process.on(type, async () => {
    try {
      console.log(`process.on ${type}`);
      await producer.disconnect();
      process.exit(0);
    } catch (_) {
      process.exit(1);
    }
  });
});

signalTraps.forEach((type) => {
  process.once(type, async () => {
    try {
      await producer.disconnect();
    } finally {
      process.kill(process.pid, type);
    }
  });
});