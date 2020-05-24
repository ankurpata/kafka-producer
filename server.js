const express = require('express')
const app = express();
const fs = require('fs');
const csv = require('async-csv');
const Kafka = require('node-rdkafka');
//console.log(Kafka.features);
//console.log(Kafka.librdkafkaVersion);

const producer = new Kafka.Producer({
    'metadata.broker.list': 'localhost:9092',
    'dr_cb': true
});

const topicName = 'CSV_PRODUCTS';

//logging debug messages, if debug is enabled
producer.on('event.log', function (log) {
    console.log(log);
});

//logging all errors
producer.on('event.error', function (err) {
    console.error('Error from producer');
    console.error(err);
});

producer.on('delivery-report', function (err, report) {
    console.log('delivery-report: ' + JSON.stringify(report));
    counter++;
});

//Wait for the ready event before producing
producer.on('ready', function (arg) {
    console.log('producer ready.' + JSON.stringify(arg));
});

producer.on('disconnected', function (arg) {
    console.log('producer disconnected. ' + JSON.stringify(arg));
});

//starting the producer
producer.connect();

app.get('/', (req, res) => res.send('Ready to send messages!'));

/**
 * Price producer
 */
app.get('/price_revision', async (req, res) => {
    const loopArr = new Array(1);
    // for (const count of loopArr) {

    // Read csv data of price upates and dispatch to respective topic
    const csvString = await fs.readFileSync('./price_revision.csv', 'utf-8');
    // Convert CSV string into rows:
    const rows = await csv.parse(csvString);
    console.log(rows, 'Read Rows CSV')
    // Dispatch topic to kafka
    const TOPIC = "PRICE_REVISION";
    const partition = -1;
    const key = 'KEY-PRICE';
    let value = Buffer.from(JSON.stringify(rows));
    producer.produce(TOPIC, partition, value, key);

    console.log('Dipatched CSV data', rows.length);
    // await fetch(uri);
    await wait(5000);
    // }
    res.json({res: "Dispatched CSV data" , len: rows.length })
    return;
});

/**
 * CSV import producer
 */
app.get('/csv', (req, res) => {

    const TOPIC = "IMPORT_CSV";
    const partition = -1;
    const key = 'KEY-1';
    let value = "UPLOAD_CSV";
    value = Buffer.from(JSON.stringify(value));

    producer.produce(TOPIC, partition, value, key);
    return;

})
app.post('/:maxMessages', function (req, res) {
    if (req.params.maxMessages) {
        console.log(req.params.maxMessages, 'reqparms');
        const maxMessages = parseInt(req.params.maxMessages) || 10;
        for (let i = 0; i < maxMessages; i++) {
            let value = new Buffer('MyProducerTest - value-' + i);
            let key = "key-" + i;
            // if partition is set to -1, librdkafka will use the default partitioner
            let partition = -1;
            producer.produce(topicName, partition, value, key);
        } // end for
    } // end if
}); // end app.post()

app.listen(4570, () => console.log('Example app listening on port 4570!'));

//OTHER  custom helper functions
/**
 *
 * @param ms
 * @returns {Promise<unknown>}
 */
async function wait(ms) {
    return new Promise(resolve => {
        setTimeout(resolve, ms);
    });
}
