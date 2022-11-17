const helper = require('node-red-node-test-helper');
const KafkaOutNode = require ('../src/rdkafka-secure-out');
const KafkaBroker = require('../src/rdkafka-secure-broker');
const {beforeEach, afterEach, it, describe, jest: jestFw} = require('@jest/globals');
helper.init(require.resolve('node-red'));



const mockProducerListeners = {};
const mockProducerGenerator = jestFw.fn(() => {
    return {
        on(event, listener) {
            mockProducerListeners[event] = listener;
            return mockProducerInstance;
        },
        disconnect: jestFw.fn().mockName('disconnect'),
        connect: jestFw.fn().mockName('connect'),
        produce: jestFw.fn().mockName('produce'),
    }
});
const mockProducerInstance = mockProducerGenerator();
const mockProducer = jestFw.fn((opts) => {
    return mockProducerInstance;
});


jestFw.mock('node-rdkafka', () => {
    const originalModule = jestFw.requireActual('node-rdkafka');
    return {
        ...originalModule,
        Producer: class {
            constructor(opts) {
                return mockProducer(opts);
            }
            static [Symbol.hasInstance](_obj) {
                return true;
            }
        }
    }
});

function getTestFlow() {
    const testFlow = [
        {
            id: 'testbroker',
            type: 'rdkafka-secure-broker',
            name: 'broker',
            broker: 'nobroker:9092',
            clientid: 'clientId',
        }, 
        {
            id: 'test',
            type: 'rdkafka-secure-out',
            name: 'kafka-out',
            broker: 'testbroker',
            keyType: 'str',
        }
    ]
    return testFlow;
}

function getDefaultOptions() {
    return {
        'client.id': 'clientId',
        'metadata.broker.list': 'nobroker:9092', 
        'retry.backoff.ms': 200,  
        'message.send.max.retries': 15,             
        'socket.keepalive.enable': true,
        'queue.buffering.max.ms': 1,
        'queue.buffering.max.messages': 100000,
        'queue.buffering.max.ms': 10,
        'batch.num.messages': 1000000,
        'api.version.request': true
    }
}

function getDefaultTestMsg() {
    const testMsg = {
        partition: 1,
        timestamp: 10,
        payload: 'testPayload',
        topic: 'testTopic',
    }
    return testMsg;
}

describe('Kafka Producer Node', () => {
    beforeEach((done) => {
        jestFw.clearAllMocks();
        helper.startServer(done);
    });

    afterEach((done) => {
        helper.unload();
        helper.stopServer(done);
    });

    it('Should show red status if no broker configured', async () => {
        await helper.load([KafkaOutNode], [{id: 'test', name: 'testOut', type: 'rdkafka-secure-out'}]);
        const n1 = await helper.getNode('test');
        n1.status.should.be.calledWithExactly({fill: 'red', shape: 'ring', text: 'missing broker configuration'});        
    });

    it('Should handle optional broker parameters', async () => {
        const flow = getTestFlow();
        const producerOptions = getDefaultOptions();
        flow.at(0)['security_protocol'] = 'sasl';
        producerOptions['security.protocol'] = 'sasl';
        flow.at(0)['sasl_mechanism'] = 'sasl-mechanism';
        producerOptions['sasl.mechanisms'] = 'sasl-mechanism';
        flow.at(0)['ssl_ca_location'] = 'location';
        producerOptions['ssl.ca.location'] = 'location';
        flow.at(0)['enable_ssl_certificate_verification'] = true;
        producerOptions['enable.ssl.certificate.verification'] = true;
        producerOptions['sasl.password'] = 'password';
        producerOptions['sasl.username'] = 'username';
        await helper.load([KafkaOutNode, KafkaBroker], flow, {testbroker: {sasl_username: 'username', sasl_password: 'password'}});
        const n1 = await helper.getNode('test');
        expect(mockProducer).toHaveBeenCalledWith(producerOptions);
        expect(mockProducerInstance.connect).toHaveBeenCalledTimes(1);
    });

    it('Should connect to Kafka if inputs are ok', async () => {
        const flow = getTestFlow();
        const producerOptions = getDefaultOptions();
        await helper.load([KafkaOutNode, KafkaBroker], flow);
        const n1 = await helper.getNode('test');
        expect(mockProducer).toHaveBeenCalledWith(producerOptions);
        expect(mockProducerInstance.connect).toHaveBeenCalledTimes(1);
    });

    it('Should show green status on Kafka ready event', async () => {
        const flow = getTestFlow();
        const producerOptions = getDefaultOptions();
        await helper.load([KafkaOutNode, KafkaBroker], flow);
        const n1 = await helper.getNode('test');
        expect(mockProducerListeners.ready).toBeDefined();
        mockProducerListeners.ready();
        expect(mockProducer).toHaveBeenCalledWith(producerOptions);
        expect(mockProducerInstance.connect).toHaveBeenCalledTimes(1);
        n1.status.should.be.calledWithExactly({fill: 'green', shape: 'dot', text: 'connected'});
    });

    it('Should show red status on Kafka event.error', async () => {
        const flow = getTestFlow();
        const producerOptions = getDefaultOptions();
        await helper.load([KafkaOutNode, KafkaBroker], flow);
        const n1 = await helper.getNode('test');
        expect(mockProducerListeners['event.error']).toBeDefined();
        mockProducerListeners['event.error']({message: 'error from producer'});
        expect(mockProducer).toHaveBeenCalledWith(producerOptions);
        expect(mockProducerInstance.connect).toHaveBeenCalledTimes(1);
        n1.status.should.be.calledWithExactly({fill: 'red', shape: 'ring', text: 'error from producer'});
    });

    it('Should show red status on Kafka connection.failure', async () => {
        const flow = getTestFlow();
        const producerOptions = getDefaultOptions();
        await helper.load([KafkaOutNode, KafkaBroker], flow);
        const n1 = await helper.getNode('test');
        expect(mockProducerListeners['connection.failure']).toBeDefined();
        mockProducerListeners['connection.failure']({message: 'error from producer'});
        expect(mockProducer).toHaveBeenCalledWith(producerOptions);
        expect(mockProducerInstance.connect).toHaveBeenCalledTimes(1);
        n1.status.should.be.calledWithExactly({fill: 'red', shape: 'ring', text: 'error from producer'});
    });
    
    it('Should not throw on send errors', async() => {
        mockProducerInstance.produce
            .mockImplementationOnce(() => {throw new Error('UnknownPartition')})
            .mockImplementationOnce(() => {throw new Error()})
        const flow = getTestFlow();
        const testMsg = getDefaultTestMsg();
        testMsg.payload = false;
        const expectedValue = Buffer.from(testMsg.payload.toString());
        await helper.load([KafkaOutNode, KafkaBroker], flow);
        const n1 = await helper.getNode('test');
        n1.receive(testMsg);
        n1.receive(testMsg);
        expect(mockProducerInstance.produce).toHaveBeenCalledTimes(2);
        expect(mockProducerInstance.produce).toHaveBeenCalledWith(testMsg.topic, testMsg.partition, expectedValue, null, testMsg.timestamp, undefined, []);
        expect(n1.error.calledTwice).toBe(true);
        expect(n1.error.firstCall.args).toContain('UnknownPartition');
    });

    describe('Partition', () => {
        
        it('Should be numeric for number in payload', async () => {
            const flow = getTestFlow();
            const testMsg = getDefaultTestMsg();
            await helper.load([KafkaOutNode, KafkaBroker], flow);
            const n1 = await helper.getNode('test');
            n1.receive(testMsg);
            expect(mockProducerInstance.produce).toHaveBeenCalledTimes(1);
            expect(mockProducerInstance.produce).toHaveBeenCalledWith(testMsg.topic, testMsg.partition, Buffer.from(testMsg.payload), null, testMsg.timestamp, undefined, []);
        });
    
        it('Should be converted string -> number for numstring in payload', async () => {
            const flow = getTestFlow();
            const testMsg = getDefaultTestMsg();
            testMsg.partition = '42';
            await helper.load([KafkaOutNode, KafkaBroker], flow);
            const n1 = await helper.getNode('test');
            n1.receive(testMsg);
            expect(mockProducerInstance.produce).toHaveBeenCalledTimes(1);
            expect(mockProducerInstance.produce).toHaveBeenCalledWith(testMsg.topic, 42, Buffer.from(testMsg.payload), null, testMsg.timestamp, undefined, []);
        });
    
        it('Should be -1 for non-numeric partition in payload', async () => {
            const flow = getTestFlow();
            const testMsg = getDefaultTestMsg();
            testMsg.partition = 'not numeric';
            await helper.load([KafkaOutNode, KafkaBroker], flow);
            const n1 = await helper.getNode('test');
            n1.receive(testMsg);
            expect(mockProducerInstance.produce).toHaveBeenCalledTimes(1);
            expect(mockProducerInstance.produce).toHaveBeenCalledWith(testMsg.topic, -1, Buffer.from(testMsg.payload), null, testMsg.timestamp, undefined, []);
        });
    
        it('Should be -1 for object partition in payload', async () => {
            const flow = getTestFlow();
            const testMsg = getDefaultTestMsg();
            testMsg.partition = {test: 'object'};
            await helper.load([KafkaOutNode, KafkaBroker], flow);
            const n1 = await helper.getNode('test');
            n1.receive(testMsg);
            expect(mockProducerInstance.produce).toHaveBeenCalledTimes(1);
            expect(mockProducerInstance.produce).toHaveBeenCalledWith(testMsg.topic, -1, Buffer.from(testMsg.payload), null, testMsg.timestamp, undefined, []);
        });
    
        it('Should be -1 for negative partition in payload', async () => {
            const flow = getTestFlow();
            const testMsg = getDefaultTestMsg();
            testMsg.partition = -10;
            await helper.load([KafkaOutNode, KafkaBroker], flow);
            const n1 = await helper.getNode('test');
            n1.receive(testMsg);
            expect(mockProducerInstance.produce).toHaveBeenCalledTimes(1);
            expect(mockProducerInstance.produce).toHaveBeenCalledWith(testMsg.topic, -1, Buffer.from(testMsg.payload), null, testMsg.timestamp, undefined, []);
        });
    
        it('Should be the number from msg.partition if both msg.partition and node config values are good', async () => {
            const flow = getTestFlow();
            flow.at(1).partition = 42;
            const testMsg = getDefaultTestMsg();
            await helper.load([KafkaOutNode, KafkaBroker], flow);
            const n1 = await helper.getNode('test');
            n1.receive(testMsg);
            expect(mockProducerInstance.produce).toHaveBeenCalledTimes(1);
            expect(mockProducerInstance.produce).toHaveBeenCalledWith(testMsg.topic, testMsg.partition, Buffer.from(testMsg.payload), null, testMsg.timestamp, undefined, []);
        });
    
        it('Should be converted string -> number for numstring in node config', async () => {
            const flow = getTestFlow();
            flow.at(1).partition = '42';
            const testMsg = getDefaultTestMsg();
            delete testMsg.partition;
            await helper.load([KafkaOutNode, KafkaBroker], flow);
            const n1 = await helper.getNode('test');
            n1.receive(testMsg);
            expect(mockProducerInstance.produce).toHaveBeenCalledTimes(1);
            expect(mockProducerInstance.produce).toHaveBeenCalledWith(testMsg.topic, 42, Buffer.from(testMsg.payload), null, testMsg.timestamp, undefined, []);
        });
    
        it('Should be -1 for non-numeric partition in node config', async () => {
            const flow = getTestFlow();
            flow.at(1).partition = 'not numeric';
            const testMsg = getDefaultTestMsg();
            delete testMsg.partition;
            await helper.load([KafkaOutNode, KafkaBroker], flow);
            const n1 = await helper.getNode('test');
            n1.receive(testMsg);
            expect(mockProducerInstance.produce).toHaveBeenCalledTimes(1);
            expect(mockProducerInstance.produce).toHaveBeenCalledWith(testMsg.topic, -1, Buffer.from(testMsg.payload), null, testMsg.timestamp, undefined, []);
        });
    
        it('Should be -1 for negative partition in node config', async () => {
            const flow = getTestFlow();
            flow.at(1).partition = -42;
            const testMsg = getDefaultTestMsg();
            delete testMsg.partition;
            await helper.load([KafkaOutNode, KafkaBroker], flow);
            const n1 = await helper.getNode('test');
            n1.receive(testMsg);
            expect(mockProducerInstance.produce).toHaveBeenCalledTimes(1);
            expect(mockProducerInstance.produce).toHaveBeenCalledWith(testMsg.topic, -1, Buffer.from(testMsg.payload), null, testMsg.timestamp, undefined, []);
        });

        it('Should be the value from msg.partition if both values are good', async () => {
            const flow = getTestFlow();
            flow.at(1).partition = 42;
            const testMsg = getDefaultTestMsg();
            await helper.load([KafkaOutNode, KafkaBroker], flow);
            const n1 = await helper.getNode('test');
            n1.receive(testMsg);
            expect(mockProducerInstance.produce).toHaveBeenCalledTimes(1);
            expect(mockProducerInstance.produce).toHaveBeenCalledWith(testMsg.topic, testMsg.partition, Buffer.from(testMsg.payload), null, testMsg.timestamp, undefined, []);
        });
    });

    describe('Key', () => {
        it('Should be string for string in payload', async () => {
            const flow = getTestFlow(); 
            const testMsg = getDefaultTestMsg();
            testMsg.key = 'testKey';
            await helper.load([KafkaOutNode, KafkaBroker], flow);
            const n1 = await helper.getNode('test');
            n1.receive(testMsg);
            expect(mockProducerInstance.produce).toHaveBeenCalledTimes(1);
            expect(mockProducerInstance.produce).toHaveBeenCalledWith(testMsg.topic, testMsg.partition, Buffer.from(testMsg.payload), testMsg.key, testMsg.timestamp, undefined, []);
        });

        it('Should be buffer for buffer in payload', async () => {
            const flow = getTestFlow(); 
            const testMsg = getDefaultTestMsg();
            testMsg.key = Buffer.from('testKey');
            await helper.load([KafkaOutNode, KafkaBroker], flow);
            const n1 = await helper.getNode('test');
            n1.receive(testMsg);
            expect(mockProducerInstance.produce).toHaveBeenCalledTimes(1);
            expect(mockProducerInstance.produce).toHaveBeenCalledWith(testMsg.topic, testMsg.partition, Buffer.from(testMsg.payload), testMsg.key, testMsg.timestamp, undefined, []);
        });

        it('Should be string from msg.key, if both msg.key and node config are good', async () => {
            const flow = getTestFlow(); 
            flow.at(1).key = 'testKey2'
            const testMsg = getDefaultTestMsg();
            testMsg.key = Buffer.from('testKey');
            await helper.load([KafkaOutNode, KafkaBroker], flow);
            const n1 = await helper.getNode('test');
            n1.receive(testMsg);
            expect(mockProducerInstance.produce).toHaveBeenCalledTimes(1);
            expect(mockProducerInstance.produce).toHaveBeenCalledWith(testMsg.topic, testMsg.partition, Buffer.from(testMsg.payload), testMsg.key, testMsg.timestamp, undefined, []);
        });

        it('Should be null if no key given in msg or node config', async () => {
            const flow = getTestFlow(); 
            delete flow.at(1).key;
            const testMsg = getDefaultTestMsg();
            delete testMsg.key;
            await helper.load([KafkaOutNode, KafkaBroker], flow);
            const n1 = await helper.getNode('test');
            n1.receive(testMsg);
            expect(mockProducerInstance.produce).toHaveBeenCalledTimes(1);
            expect(mockProducerInstance.produce).toHaveBeenCalledWith(testMsg.topic, testMsg.partition, Buffer.from(testMsg.payload), null, testMsg.timestamp, undefined, []);
        });

        it('Should be null if node config key is empty string', async () => {
            const flow = getTestFlow(); 
            flow.at(1).key = '';
            const testMsg = getDefaultTestMsg();
            delete testMsg.key;
            await helper.load([KafkaOutNode, KafkaBroker], flow);
            const n1 = await helper.getNode('test');
            n1.receive(testMsg);
            expect(mockProducerInstance.produce).toHaveBeenCalledTimes(1);
            expect(mockProducerInstance.produce).toHaveBeenCalledWith(testMsg.topic, testMsg.partition, Buffer.from(testMsg.payload), null, testMsg.timestamp, undefined, []);
        });

        it('Should be the key from node config, if it is a non-empty string', async () => {
            const flow = getTestFlow(); 
            flow.at(1).key = 'testKey';
            const testMsg = getDefaultTestMsg();
            delete testMsg.key;
            await helper.load([KafkaOutNode, KafkaBroker], flow);
            const n1 = await helper.getNode('test');
            n1.receive(testMsg);
            expect(mockProducerInstance.produce).toHaveBeenCalledTimes(1);
            expect(mockProducerInstance.produce).toHaveBeenCalledWith(testMsg.topic, testMsg.partition, Buffer.from(testMsg.payload), 'testKey', testMsg.timestamp, undefined, []);
        });

        it('Should be null, if key given only in node config as empty string', async () => {
            const flow = getTestFlow(); 
            flow.at(1).key = '';
            const testMsg = getDefaultTestMsg();
            delete testMsg.key;
            await helper.load([KafkaOutNode, KafkaBroker], flow);
            const n1 = await helper.getNode('test');
            n1.receive(testMsg);
            expect(mockProducerInstance.produce).toHaveBeenCalledTimes(1);
            expect(mockProducerInstance.produce).toHaveBeenCalledWith(testMsg.topic, testMsg.partition, Buffer.from(testMsg.payload), null, testMsg.timestamp, undefined, []);
        });

        it('Should be the buffer from node config, if it is given', async () => {
            const flow = getTestFlow(); 
            flow.at(1).key = '[10,"20" , 30  ]'; //intentionally badly formatted
            flow.at(1).keyType = 'bin';
            const testMsg = getDefaultTestMsg();
            delete testMsg.key;
            await helper.load([KafkaOutNode, KafkaBroker], flow);
            const n1 = await helper.getNode('test');
            n1.receive(testMsg);
            expect(mockProducerInstance.produce).toHaveBeenCalledTimes(1);
            expect(mockProducerInstance.produce).toHaveBeenCalledWith(testMsg.topic, testMsg.partition, Buffer.from(testMsg.payload), Buffer.from([10,20,30]), testMsg.timestamp, undefined, []);
        });

        it('Should be the null if invalid buffer from node config', async () => {
            const flow = getTestFlow(); 
            flow.at(1).key = '[';
            flow.at(1).keyType = 'bin';
            const testMsg = getDefaultTestMsg();
            delete testMsg.key;
            await helper.load([KafkaOutNode, KafkaBroker], flow);
            const n1 = await helper.getNode('test');
            n1.receive(testMsg);
            expect(mockProducerInstance.produce).toHaveBeenCalledTimes(1);
            expect(mockProducerInstance.produce).toHaveBeenCalledWith(testMsg.topic, testMsg.partition, Buffer.from(testMsg.payload), null, testMsg.timestamp, undefined, []);
        });
    });

    describe('Topic', () => {
        it('Should be string from msg if defined both in msg and node config', async () => {
            const flow = getTestFlow(); 
            flow.at(1).topic = 'anotherTopic';
            const testMsg = getDefaultTestMsg();
            await helper.load([KafkaOutNode, KafkaBroker], flow);
            const n1 = await helper.getNode('test');
            n1.receive(testMsg);
            expect(mockProducerInstance.produce).toHaveBeenCalledTimes(1);
            expect(mockProducerInstance.produce).toHaveBeenCalledWith(testMsg.topic, testMsg.partition, Buffer.from(testMsg.payload), null, testMsg.timestamp, undefined, []);
        });

        it('Should be string from node config if defined only in node config', async () => {
            const flow = getTestFlow(); 
            flow.at(1).topic = 'anotherTopic';
            const testMsg = getDefaultTestMsg();
            delete testMsg.topic;
            await helper.load([KafkaOutNode, KafkaBroker], flow);
            const n1 = await helper.getNode('test');
            n1.receive(testMsg);
            expect(mockProducerInstance.produce).toHaveBeenCalledTimes(1);
            expect(mockProducerInstance.produce).toHaveBeenCalledWith('anotherTopic', testMsg.partition, Buffer.from(testMsg.payload), null, testMsg.timestamp, undefined, []);
        });

        it('Should lead to an error if topic is empty string', async () => {
            const flow = getTestFlow(); 
            const testMsg = getDefaultTestMsg();
            testMsg.topic = '';
            await helper.load([KafkaOutNode, KafkaBroker], flow);
            const n1 = await helper.getNode('test');
            n1.receive(testMsg);
            expect(mockProducerInstance.produce).toHaveBeenCalledTimes(0);
            expect(n1.error.calledOnce).toBe(true);
        });

        it('Should lead to an error if neither msg.topic nor node config topic is set', async () => {
            const flow = getTestFlow();
            delete flow.at(1).topic; 
            const testMsg = getDefaultTestMsg();
            delete testMsg.topic;
            await helper.load([KafkaOutNode, KafkaBroker], flow);
            const n1 = await helper.getNode('test');
            n1.receive(testMsg);
            expect(mockProducerInstance.produce).toHaveBeenCalledTimes(0);
            expect(n1.error.calledOnce).toBe(true);
        });
    });

    describe('Sent Value', () => {
        it('Should be buffer if msg.payload is a buffer', async () => {
            const flow = getTestFlow();
            const testMsg = getDefaultTestMsg();
            testMsg.payload = Buffer.from('testPayload');
            await helper.load([KafkaOutNode, KafkaBroker], flow);
            const n1 = await helper.getNode('test');
            n1.receive(testMsg);
            expect(mockProducerInstance.produce).toHaveBeenCalledTimes(1);
            expect(mockProducerInstance.produce).toHaveBeenCalledWith(testMsg.topic, testMsg.partition, testMsg.payload, null, testMsg.timestamp, undefined, []);
        });

        it('Should be buffer if msg.payload is a JSON-parseable Object', async () => {
            const flow = getTestFlow();
            const testMsg = getDefaultTestMsg();
            testMsg.payload = {test: 'object'};
            const expectedValue = Buffer.from(JSON.stringify(testMsg.payload))
            await helper.load([KafkaOutNode, KafkaBroker], flow);
            const n1 = await helper.getNode('test');
            n1.receive(testMsg);
            expect(mockProducerInstance.produce).toHaveBeenCalledTimes(1);
            expect(mockProducerInstance.produce).toHaveBeenCalledWith(testMsg.topic, testMsg.partition, expectedValue, null, testMsg.timestamp, undefined, []);
        });

        it('Should be null if msg.payload is a non-parseable Object', async () => {
            const flow = getTestFlow();
            const testMsg = getDefaultTestMsg();
            const obj1 = {ref: undefined};
            obj1.ref = obj1; //circular reference
            testMsg.payload = obj1;
            await helper.load([KafkaOutNode, KafkaBroker], flow);
            const n1 = await helper.getNode('test');
            n1.receive(testMsg);
            expect(mockProducerInstance.produce).toHaveBeenCalledTimes(1);
            expect(mockProducerInstance.produce).toHaveBeenCalledWith(testMsg.topic, testMsg.partition, null, null, testMsg.timestamp, undefined, []);
            expect(n1.warn.calledTwice).toBe(true);
        });

        it('Should be buffer if msg.payload is a number', async () => {
            const flow = getTestFlow();
            const testMsg = getDefaultTestMsg();
            testMsg.payload = 42;
            const expectedValue = Buffer.from('42');
            await helper.load([KafkaOutNode, KafkaBroker], flow);
            const n1 = await helper.getNode('test');
            n1.receive(testMsg);
            expect(mockProducerInstance.produce).toHaveBeenCalledTimes(1);
            expect(mockProducerInstance.produce).toHaveBeenCalledWith(testMsg.topic, testMsg.partition, expectedValue, null, testMsg.timestamp, undefined, []);
        });

        it('Should be buffer if msg.payload is a boolean', async () => {
            const flow = getTestFlow();
            const testMsg = getDefaultTestMsg();
            testMsg.payload = false;
            const expectedValue = Buffer.from(testMsg.payload.toString());
            await helper.load([KafkaOutNode, KafkaBroker], flow);
            const n1 = await helper.getNode('test');
            n1.receive(testMsg);
            expect(mockProducerInstance.produce).toHaveBeenCalledTimes(1);
            expect(mockProducerInstance.produce).toHaveBeenCalledWith(testMsg.topic, testMsg.partition, expectedValue, null, testMsg.timestamp, undefined, []);
        });
    });

    describe('Timestamp', () => {

        beforeEach(() => {
            Date.now = jestFw.fn(() => {return 42});
        });

        it('Should set timestamp to Date.now() if no msg.timestamp given', async () => {
            const flow = getTestFlow();
            const testMsg = getDefaultTestMsg();
            delete testMsg.timestamp;
            await helper.load([KafkaOutNode, KafkaBroker], flow);
            const n1 = await helper.getNode('test');
            n1.receive(testMsg);
            expect(mockProducerInstance.produce).toHaveBeenCalledTimes(1);
            expect(mockProducerInstance.produce).toHaveBeenCalledWith(testMsg.topic, testMsg.partition, Buffer.from(testMsg.payload), null, 42, undefined, []);
        });


        it('Should lead to a warning if msg.timestamp is set but not numeric', async () => {
            const flow = getTestFlow();
            const testMsg = getDefaultTestMsg();
            testMsg.timestamp = 'no timestamp';
            await helper.load([KafkaOutNode, KafkaBroker], flow);
            const n1 = await helper.getNode('test');
            n1.receive(testMsg);
            expect(mockProducerInstance.produce).toHaveBeenCalledTimes(1);
            expect(mockProducerInstance.produce).toHaveBeenCalledWith(testMsg.topic, testMsg.partition, Buffer.from(testMsg.payload), null, 42, undefined, []);
            expect(n1.warn.calledOnce).toBe(true);
        });
    });

    describe('Headers', () => {
        it('Should be empty if msg.headers is not defined', async () => {
            const flow = getTestFlow();
            const testMsg = getDefaultTestMsg();
            delete testMsg.headers;
            await helper.load([KafkaOutNode, KafkaBroker], flow);
            const n1 = await helper.getNode('test');
            n1.receive(testMsg);
            expect(mockProducerInstance.produce).toHaveBeenCalledTimes(1);
            expect(mockProducerInstance.produce).toHaveBeenCalledWith(testMsg.topic, testMsg.partition, Buffer.from(testMsg.payload), null, testMsg.timestamp, undefined, []);
        });

        it('Should be empty if msg.headers is null', async () => {
            const flow = getTestFlow();
            const testMsg = getDefaultTestMsg();
            testMsg.headers = null;
            await helper.load([KafkaOutNode, KafkaBroker], flow);
            const n1 = await helper.getNode('test');
            n1.receive(testMsg);
            expect(mockProducerInstance.produce).toHaveBeenCalledTimes(1);
            expect(mockProducerInstance.produce).toHaveBeenCalledWith(testMsg.topic, testMsg.partition, Buffer.from(testMsg.payload), null, testMsg.timestamp, undefined, []);
        });

        it('Should convert key:string | value:string object -> header array', async () => {
            const flow = getTestFlow();
            const testMsg = getDefaultTestMsg();
            testMsg.headers = {test: 'headers'};
            await helper.load([KafkaOutNode, KafkaBroker], flow);
            const n1 = await helper.getNode('test');
            n1.receive(testMsg);
            expect(mockProducerInstance.produce).toHaveBeenCalledTimes(1);
            expect(mockProducerInstance.produce).toHaveBeenCalledWith(testMsg.topic, testMsg.partition, Buffer.from(testMsg.payload), null, testMsg.timestamp, undefined, [testMsg.headers]);
        });

        it('Should convert key:string | value:Buffer object -> header array', async () => {
            const flow = getTestFlow();
            const testMsg = getDefaultTestMsg();
            testMsg.headers = {test: Buffer.from('headers')};
            await helper.load([KafkaOutNode, KafkaBroker], flow);
            const n1 = await helper.getNode('test');
            n1.receive(testMsg);
            expect(mockProducerInstance.produce).toHaveBeenCalledTimes(1);
            expect(mockProducerInstance.produce).toHaveBeenCalledWith(testMsg.topic, testMsg.partition, Buffer.from(testMsg.payload), null, testMsg.timestamp, undefined, [testMsg.headers]);
        });

        it('Should use valid header array from msg.headers', async () => {
            const flow = getTestFlow();
            const testMsg = getDefaultTestMsg();
            testMsg.headers = [{test: 'test'}, {test2: Buffer.from('headers')}];
            await helper.load([KafkaOutNode, KafkaBroker], flow);
            const n1 = await helper.getNode('test');
            n1.receive(testMsg);
            expect(mockProducerInstance.produce).toHaveBeenCalledTimes(1);
            expect(mockProducerInstance.produce).toHaveBeenCalledWith(testMsg.topic, testMsg.partition, Buffer.from(testMsg.payload), null, testMsg.timestamp, undefined, testMsg.headers);
        });

        it('Should filter invalid entries from msg.headers', async () => {
            const flow = getTestFlow();
            const testMsg = getDefaultTestMsg();
            testMsg.headers = [{test: 'test'}, {test2: Buffer.from('headers')}, {test3: 42}, {test4: () => {return 'fail'}}, {test5: false}];
            expectedHeaders = testMsg.headers.slice(0,2);
            await helper.load([KafkaOutNode, KafkaBroker], flow);
            const n1 = await helper.getNode('test');
            n1.receive(testMsg);
            expect(mockProducerInstance.produce).toHaveBeenCalledTimes(1);
            expect(mockProducerInstance.produce).toHaveBeenCalledWith(testMsg.topic, testMsg.partition, Buffer.from(testMsg.payload), null, testMsg.timestamp, undefined, expectedHeaders);
        });
    });

    




})