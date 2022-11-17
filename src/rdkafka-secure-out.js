"use strict";
module.exports = function(RED) {
    const Kafka = require('node-rdkafka');

    function RdKafkaOutNode(n) {
        RED.nodes.createNode(this, n);
        this.topic = n.topic;
        this.broker = n.broker;
        this.key = n.key;
        this.keyType = n.keyType;
        this.partition = Number(n.partition);
        this.brokerConfig = RED.nodes.getNode(this.broker);
        this.producer = undefined;
        const node = this;
        

        node.status({
            fill: "red",
            shape: "ring",
            text: "disconnected"
        });

        if (!node.brokerConfig) {
            const text = 'missing broker configuration';
            node.error(text);
            node.status({fill: 'red', shape: 'ring', text});
            return;
        }           

        try {
            const options = {
                'client.id': node.brokerConfig.clientid,
                'metadata.broker.list': node.brokerConfig.broker,
                //'compression.codec': 'gzip',
                'retry.backoff.ms': 200,
                'message.send.max.retries': 15,
                'socket.keepalive.enable': true,
                'queue.buffering.max.messages': 100000,
                'queue.buffering.max.ms': 10,
                'batch.num.messages': 1000000,
                'api.version.request': true  //added to force 0.10.x style timestamps on all messages
            };
            if (node.brokerConfig.security_protocol) {
                options['security.protocol'] = node.brokerConfig.security_protocol;
            }
            if (node.brokerConfig.sasl_mechanism) {
                options['sasl.mechanisms'] = node.brokerConfig.sasl_mechanism;
            }
            if (node.brokerConfig.credentials.sasl_username) {
                options['sasl.username'] = node.brokerConfig.credentials.sasl_username;
            }
            if (node.brokerConfig.credentials.sasl_password) {
                options['sasl.password'] = node.brokerConfig.credentials.sasl_password;
            }
            if (node.brokerConfig.ssl_support_enabled && node.brokerConfig.ssl_ca_location) {
                options['ssl.ca.location'] = node.brokerConfig.ssl_ca_location;
            }
            if (node.brokerConfig.ssl_support_enabled && typeof node.brokerConfig.enable_ssl_certificate_verification !== 'undefined') {
                options['enable.ssl.certificate.verification'] = node.brokerConfig.enable_ssl_certificate_verification;
            }

            node.producer = new Kafka.Producer(options);

            node.producer
                .on('ready', () => {
                    node.log('Connection to Kafka broker(s) is ready');
                    // Wait for the ready event before proceeding
                    node.status({
                        fill: "green",
                        shape: "dot",
                        text: "connected"
                    });
                })
                .on('event.error', (err) => {
                    node.error(`Error from producer: ${err.message}`, err);
                    node.status({
                        fill: "red",
                        shape: "ring",
                        text: err.message,
                    });
                })
                .on('connection.failure', (err) => {
                    node.error(`Connection error from producer: ${err.message}`, err);
                    node.status({
                        fill: 'red',
                        shape: 'ring',
                        text: err.message,
                    })
                })
                .connect(); //connect manually
        } catch (err) {
            node.error('Error creating Kafka producer', err);
            node.status({
                text: 'error creating producer',
                fill: 'red',
                shape: 'ring',
            });
        }

        node.on('input', (msg) => {
            
            if (!(node.producer instanceof Kafka.Producer)) {
                node.error('Producer not initialized');
                return;
            }
            //handle different payload types including JSON object
            let partition, key, topic;
            let timestamp = Date.now();
            let value = null;

            //console.log('msg.partition', typeof msg.partition, Number.isInteger(msg.partition));
            //console.log('node.partition', typeof node.partition, Number.isInteger(node.partition));

            //set the partition
            if (msg.partition && Number.isInteger(Number(msg.partition)) && Number(msg.partition) >= 0) {
                partition = Number(msg.partition);
            } else if (node.partition && Number.isInteger(node.partition) && Number(node.partition) >= 0){
                partition = Number(node.partition);
            } else {
                partition = -1;
            }

            //set the key
            let nodeKey = undefined;
            try {
                nodeKey = node.key && node.key.length > 0 ? RED.util.evaluateNodeProperty(node.key, node.keyType, node) : undefined;
            } catch {
                nodeKey = undefined;
            }
            const nonemptyString = typeof nodeKey === 'string' && nodeKey.length > 0;
            if ( msg.key && (typeof msg.key === 'string' || Buffer.isBuffer(msg.key)) ) {
                key = msg.key;
            } else if ( nodeKey && (nonemptyString || Buffer.isBuffer(nodeKey))) {
                key = nodeKey;
            } else {
                key = null;
            }

            //set the topic
            topic = msg.topic ?? node.topic;
            if (typeof topic !== 'string' || topic.length === 0) {
                node.error('topic must be a non-empty string. Either set msg.topic or provide it in the node');
                return;
            }

            //set the value
            if (Buffer.isBuffer(msg.payload)) {
                value = msg.payload;
            } else if (typeof msg.payload === 'object') {
                try {
                    const stringifiedPayload = JSON.stringify(msg.payload);
                    value = Buffer.from(stringifiedPayload);
                } catch (err) {
                    //JSON.stringify might fail e.g.for circular structures
                    node.warn('could not convert object to buffer. Sending empty payload.');
                    node.warn(err);
                    value = null;
                } 
            } else if (msg.payload !== undefined && msg.payload !== null) {
                //e.g. numeric payload
                try {
                    const stringPayload = msg.payload.toString();
                    value = Buffer.from(stringPayload);
                } catch (err) {
                    node.warn('could not convert payload to buffer. Sending empty payload.');
                    node.warn(err);
                    value = null;
                }
            }

            //set the timestamp
            if (typeof msg.timestamp === 'number' && Number.isInteger(msg.timestamp)) {
                timestamp = msg.timestamp;
            } else if (msg.timestamp !== undefined) {
                node.warn('Ignoring the following invalid timestamp on message:' + msg.timestamp);
            }

            //set headers
            const headers = getHeaders(msg.headers);

            try {
                node.producer.produce(
                    topic,
                    partition,
                    value,
                    key,                                  
                    timestamp,
                    undefined,
                    headers,
                );
            }  catch (err) {
                node.error(err?.message && err.message.length ? err.message : 'send error - check if topic/partition combination exists');
            }  
            
        });
        
        node.on('close', () => {
            node.status({
                fill: "red",
                shape: "ring",
                text: "disconnected"
            });
            if (node.producer instanceof Kafka.Producer) {
                node.producer.disconnect();
            }
        });
    }
    RED.nodes.registerType("rdkafka-secure-out", RdKafkaOutNode);

    /**
     * 
     * @param {any} potentialHeaders object to check if it makes for valid Kafka header(s)
     * @returns {MessageHeader[]}, MessageHeader array, may be empty 
     */
     function getHeaders(potentialHeaders) {
        const singleHeaderOk = (header) => {
            if (typeof header !== 'object' || header === null) {
                return false;
            }
            const keys = Object.keys(header);
            return keys.every((key) => typeof key === 'string' && (typeof header[key] === 'string' || Buffer.isBuffer(header[key])));
        } 
        if (Array.isArray(potentialHeaders)) {
            return potentialHeaders.filter((header) => singleHeaderOk(header));
        } else if (typeof potentialHeaders === 'object') {
            return singleHeaderOk(potentialHeaders) ? [potentialHeaders] : [];
        } else {
            return [];
        }
    }


}