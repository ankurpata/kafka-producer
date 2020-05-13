const express = require('express')
const app = express();
const fs = require('fs');

const csv = require('csv-parser');

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

app.get('/', (req, res) => res.send('Ready to send messages!'))
app.get('/csv', (req, res) => {

    fs.createReadStream('data.csv')
        .pipe(csv())
        .on('data', (row, key) => {
            console.log(row, 'row');
            let value = Buffer.from(JSON.stringify(row));
            let key = "key-" + key;
            // if partition is set to -1, librdkafka will use the default partitioner
            const partition = -1;
            producer.produce(topicName, partition, value, key);
            console.log(row);
        })
        .on('end', () => {
            console.log('CSV file successfully processed');
        });

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

app.listen(4570, () => console.log('Example app listening on port 4570!'))
