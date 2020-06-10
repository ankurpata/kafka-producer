const express = require('express')
const app = express();
const fs = require('fs');
const csv = require('async-csv');
const axios = require('axios');
const Kafka = require('node-rdkafka');
//console.log(Kafka.features);
//console.log(Kafka.librdkafkaVersion);

const producer = new Kafka.Producer({
    'metadata.broker.list': 'localhost:9092',
    'message.max.bytes': '15728640',
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
    const csvString = await fs.readFileSync('./price_revision2.csv', 'utf-8');
    // Convert CSV string into rows:
    const rows = await csv.parse(csvString);
    let i = 1;
    const intNo = (new Date()).getTime().toString();

    /**
     * Log time before dispatching
     */
    const logParams = {
        "iterationName": "price_revision_insert",
        "iterationNumber": intNo,
        "numRecords": rows.length,
        "execTime1": -1,
        "startTime": new Date().toISOString(),
        "endTime": new Date().toISOString(),
        "batchNumber": 1,
        "itemsPerBatch": rows.length,
        "execTime2": "-1",
        "platform": "reaction"
    };
    try {

        //Async post and do not wait for response.
        const {data: logRes} = await axios.post('https://us-central1-stylishopdev.cloudfunctions.net/perf-monitor', logParams);
        console.log(rows.length, logParams, 'Read Rows CSV', ". Dispatching price revision data. logRes: ", logRes);
    } catch (e) {
        console.log(e.message, 'Error logging')
    }

    // Dispatch topic to kafka
    const TOPIC = "PRICE_REVISION";
    const partition = -1;
    const key = 'KEY-PRICE';
    let payload = {data: rows, intNo};
    payload = Buffer.from(JSON.stringify(payload));

    producer.produce(TOPIC, partition, payload, key);

    console.log('Dipatched CSV data', rows.length);
    await wait(5000);
    res.json({res: "Dispatched CSV data", len: rows.length});
    return;
});

/**
 * CSV import producer
 */
app.get('/csv', async (req, res) => {

    const csvSize = req.query.size;
    /**
     * Log time before dispatching
     */
    const intNo = (new Date()).getTime().toString();
    const logParams = {
        "iterationName": "csv_produts_insert",
        "iterationNumber": intNo,
        "numRecords": 0,
        "execTime1": -1,
        "startTime": new Date().toISOString(),
        "endTime": new Date().toISOString(),
        "batchNumber": 1,
        "itemsPerBatch": 0,
        "execTime2": "-1",
        "platform": "reaction"
    };
    try {

        //Async post and do not wait for response.
        const {data: logRes} = await axios.post('https://us-central1-stylishopdev.cloudfunctions.net/perf-monitor', logParams);
        console.log(logParams, 'Read Rows CSV', ". Dispatching Product CSV data. logRes: ", logRes);
    } catch (e) {
        console.log(e.message, 'Error logging')
    }

    const TOPIC = "IMPORT_CSV";
    const partition = -1;
    const key = 'KEY-1';
    let value = csvSize == 'big' ? "UPLOAD_CSV_BIG" : "UPLOAD_CSV";
    let payload = {value, intNo};
    payload = Buffer.from(JSON.stringify(payload));

    producer.produce(TOPIC, partition, payload, key);
    res.json({res: "Dispatched CSV data"});
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
