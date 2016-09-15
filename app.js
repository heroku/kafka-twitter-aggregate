'use strict';

const _ = require('lodash');

// standard JSON library cannot handle size of numeric twitter ID values
const JSONbig = require('json-bigint');

const Bacon = require('baconjs');

const webServer = require('./simple-web-server').startServer();

const Kafka = require('no-kafka');
const brokerUrls = process.env.KAFKA_URL.replace(/\+ssl/g,'');
const configTopic = process.env.KAFKA_CONFIG_TOPIC;
const consumerTopicBase = process.env.KAFKA_CONSUMER_TOPIC;

const ONE_SECOND = 1000;
const ONE_MINUTE = ONE_SECOND * 60;
const TEN_MINUTE = ONE_MINUTE * 10;

/*
 * TODO: replace the env var by reading the comma-separated list of terms from the 'config' topic
 *   'config' topic does not yet have terms in it because I haven't figured out how to
 *   write the terms to the 'config' topic on startup of the the 'ingest' producer client
 */
// const potentialKeywords = process.env.TWITTER_TRACK_TERMS.toLowerCase().split(',');
/*
let potentialKeywords = [];
*/

/*
 * Connect to topic as consumer
 * on message,
 *
 */



// Configure consumer client
const consumer = new Kafka.SimpleConsumer({
    idleTimeout: 100,
    clientId: 'twitter-aggregate-consumer',
    connectionString: brokerUrls,
    ssl: {
      certFile: './client.crt',
      keyFile: './client.key'
    }
});

// Configure producer client
const producer = new Kafka.Producer({
    clientId: 'tweet-aggregate-producer',
    connectionString: brokerUrls,
    ssl: {
      certFile: './client.crt',
      keyFile: './client.key'
    }
});

/*
 * Startup producer followed by consumer
 *
 */
return producer.init().then(function() {
    console.log('Producer connected.');
    return consumer.init().then(function () {
        console.log('Consumer connected.');

        const consumerTopic = `${consumerTopicBase}-keyword`;

        console.log('Consuming from topic:', consumerTopic);

        // Create Bacon stream from incoming Kafka messages
        const stream = Bacon.fromBinder(function(sink) {
            function dataHandler(messageSet, topic, partition) {
                messageSet.forEach(function (m) {
                    sink(JSONbig.parse(m.message.value.toString('utf8')));
                });
            }
            consumer.subscribe(consumerTopic, dataHandler);

            return function() {
                consumer.unsubscribe(consumerTopic);
            }
        });

        // Get running message count
        let count = stream.scan(0, function(acc) {
            return ++acc;
        });

        // Sample running count every second
        let sampledCountStream = count.sample(ONE_SECOND);

        let oneMinBuffer = [];
        let tenMinBuffer = [];
        let lastCount    = 0;

        // For every value in sampledCountStream,
        // do some calculations and then produce a message
        sampledCountStream.onValue(function(value) {
            let diff = value - lastCount;
            lastCount = value;

            oneMinBuffer.push(diff);
            tenMinBuffer.push(diff);

            if (oneMinBuffer.length > 60 ) oneMinBuffer.shift();
            if (tenMinBuffer.length > 600) tenMinBuffer.shift();

            let rate    = diff / (ONE_SECOND/1000);
            let rate60  = oneMinBuffer.length < 60  ? null : _.mean(oneMinBuffer);
            let rate600 = tenMinBuffer.length < 600 ? null : _.mean(tenMinBuffer);

            let msg = {
                time: Date.now(),
                count: value,
                avgPerSecond: rate,
                avgPer60Seconds: rate60,
                avgPer600Seconds: rate600
            }

            producer.send({
                topic: `${consumerTopicBase}-aggregate`,
                partition: 0,
                message: {
                    value: JSONbig.stringify(msg)
                }
            });
        });
    });
});
