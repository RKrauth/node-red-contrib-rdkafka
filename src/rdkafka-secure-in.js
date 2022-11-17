"use strict";
module.exports = function(RED) {
    const Kafka = require('node-rdkafka');

    function RdKafkaInNode(n) {
        RED.nodes.createNode(this, n);
        this.topic = n.topic;
        this.broker = n.broker;
        this.cgroup = n.cgroup;
        this.cgroupType = n.cgroupType;
        this.autocommit = n.autocommit;
        this.payloadType = n.payloadType;
        this.keyType = n.keyType;
        this.headerType = n.headerType;
        this.brokerConfig = RED.nodes.getNode(this.broker);
        this.consumer = undefined;
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

        if (node.topic === undefined || node.topic === '') {
            const text = 'missing topic';
            node.error(text);
            node.status({fill: 'red', shape: 'ring', text});
            return;
        }

        try {
            // create node-rdkafka consumer
            let nodeCgroup = 'node-red-rdkafka-secure';
            try {
                nodeCgroup = node.cgroup && node.cgroup.length > 0 ? RED.util.evaluateNodeProperty(node.cgroup, node.cgroupType, node) : nodeCgroup;
            } catch {
                //nothing to do, default value already set
            }
            const options = {
                'group.id': nodeCgroup,
                'client.id': node.brokerConfig.clientid,
                'metadata.broker.list': node.brokerConfig.broker,                    
                'socket.keepalive.enable': true,
                'enable.auto.commit': node.autocommit,
                'queue.buffering.max.ms': 1,
                'fetch.min.bytes': 1,
                'fetch.wait.max.ms': 1,          //librdkafka recommendation for low latency
                'fetch.error.backoff.ms': 100,   //librdkafka recommendation for low latency
                'api.version.request': true
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
            const {'sasl.password': _password, 'sasl.username': _username, ...otherOptions} = options;
            node.log('options\n' + JSON.stringify(otherOptions, undefined, 2));
            node.log('brokerConfig\n' + JSON.stringify(node.brokerConfig, undefined, 2));

            node.consumer = new Kafka.KafkaConsumer(options, {});

            // consumer event handlers
            node.consumer
                .on('ready', () => {
                    node.status({
                        fill: "green",
                        shape: "dot",
                        text: "connected"
                    });
                    if (node.consumer instanceof Kafka.KafkaConsumer) {
                        node.consumer.subscribe([node.topic]);
                        node.consumer.consume();
                        node.log('Created consumer subscription on topic = ' + node.topic);
                    }
                })
                .on('data', (data) => {
                    // Output the actual message contents
                    const msg = {
                        topic: data.topic,
                        offset: data.offset,
                        partition: data.partition,
                        size: data.size,
                        timestamp: data.timestamp,
                    };
                    msg.payload = transformValue(data.value, node.payloadType);
                    const key = transformKey(data.key, node.keyType);
                    if (key) {
                        msg.key = key;
                    }
                    const headers = transformHeaders(data.headers, node.headerType);
                    if (headers.length > 0) {
                        msg.headers = headers;
                    }
                    node.send(msg);
                    
                })
                .on('event.error', (err) => {
                    node.error(`Error in kafka consumer, code=${err.code}, msg=${err.message}`, err);
                    node.status({fill: 'red', shape: 'ring', text: err.message});
                })
                .on('connection.failure', (err) => {
                    node.error(`Connection failure in Kafka consumer, code=${err.code}, msg=${err.message}`, err);
                    node.status({fill: 'red', shape: 'ring', text: err.message});
                })
                .connect(); //connect in flowing mode
        } catch (err) {
            node.error('Error creating consumer connection', err);
            node.status({
                text: 'Error creating consumer',
                fill: 'red',
                shape: 'ring',
            });
        }
        
        node.on('close', () => {
            node.status({
                fill: "red",
                shape: "ring",
                text: "disconnected"
            });
            if (node.consumer instanceof Kafka.KafkaConsumer) {
                node.consumer.unsubscribe().disconnect();
            }
        });
    }
    RED.nodes.registerType("rdkafka-secure-in", RdKafkaInNode);



    /**
     * 
     * @param {Kafka.MessageValue} potentialValue 
     * @param {string} payloadType 
     * @returns {string | Buffer} payload 
     */
    function transformValue(potentialValue, payloadType) {
        if (potentialValue !== null) {
            //return buffer payload or convert buffer payload to string
            return payloadType === 'Buffer' ? potentialValue : potentialValue.toString();
        } 
        else {
            //payload is null, return empty buffer or empty string
            return payloadType === 'Buffer' ? Buffer.from('') : '';  
        }
    }

    /**
     * 
     * @param {Kafka.MessageKey} potentialKey 
     * @param {string} keyType 
     * @returns {string | Buffer | undefined} transformed key
     */
    function transformKey(potentialKey, keyType) {
        if (typeof potentialKey === 'string' && keyType === 'Buffer') {
            return Buffer.from(potentialKey); //transform string -> buffer
        } else if (Buffer.isBuffer(potentialKey) && keyType === 'String') {
            return potentialKey.toString(); //transform buffer -> string
        } else if (typeof potentialKey === 'string' || Buffer.isBuffer(potentialKey)) {
            return potentialKey; //no transform; string -> string, buffer -> buffer
        } else {
            return undefined; //null -> undefined, undefined -> undefined
        }
    }

    /**
     * 
     * @param {Kafka.MessageHeader[] | undefined} potentialHeaders 
     * @param {string} headerType 
     * @returns {Kafka.MessageHeader[]} messageHeader array
     */
    function transformHeaders(potentialHeaders, headerType) {
        const headerArray = potentialHeaders ?? [];
        return headerArray.map((header) => {
            if (headerType === 'String') {
                const transformedHeader = {};
                Object.keys(header).forEach((key) => {
                    //transform buffer -> string where necessary
                    transformedHeader[key] = typeof header[key] === 'string' ? header[key] : header[key].toString();
                });
                return transformedHeader;
            } else if (headerType === 'Buffer') {
                const transformedHeader = {};
                Object.keys(header).forEach((key) => {
                    //transform string -> buffer where necessary
                    transformedHeader[key] = typeof header[key] === 'string' ? Buffer.from(header[key]) : header[key];
                });
                return transformedHeader;
            } else {
                //no transform
                return header;
            }
        
    });
    }

};
