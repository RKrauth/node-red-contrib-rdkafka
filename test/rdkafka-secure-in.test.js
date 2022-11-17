const helper = require('node-red-node-test-helper');
const KafkaInNode = require ('../src/rdkafka-secure-in');
const KafkaBroker = require('../src/rdkafka-secure-broker');
const {beforeEach, afterEach, it, describe, expect, jest: jestFw} = require('@jest/globals');
helper.init(require.resolve('node-red'));



const mockConsumerListeners = {};
const mockConsumerGenerator = jestFw.fn(() => {
    return {
        on(event, listener) {
            mockConsumerListeners[event] = listener;
            return mockConsumerInstance;
        },
        subscribe: jestFw.fn().mockName('subscribe'),
        unsubscribe: jestFw.fn().mockName('unsubscribe'),
        disconnect: jestFw.fn().mockName('disconnect'),
        connect: jestFw.fn().mockName('connect'),
        consume: jestFw.fn().mockName('consume'),
    }
});
const mockConsumerInstance = mockConsumerGenerator();
const mockConsumer = jestFw.fn((_opts) => {
    return mockConsumerInstance;
});


jestFw.mock('node-rdkafka', () => {
    const originalModule = jestFw.requireActual('node-rdkafka');
    return {
        ...originalModule,
        KafkaConsumer: class {
            constructor(opts) {
                return mockConsumer(opts);
            }
            static [Symbol.hasInstance](_obj) {
                return true;
            }
        }
    }
})


function getTestFlow(headerType, keyType, payloadType) {
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
            type: 'rdkafka-secure-in',
            name: 'kafka-in',
            broker: 'testbroker',
            headerType,
            keyType,
            payloadType,
            topic: 'test_topic',
            autocommit: true,
        }
    ]
    return testFlow;
}

function getTestData() {
    return {
        topic: 'testTopic',
        offset: 1,
        partition: 1,
        size: 1,
        timestamp: 1,
        value: null,
    }
}

function getDefaultOptions() {
    return {
        'group.id': 'node-red-rdkafka-secure',
        'client.id': 'clientId',
        'metadata.broker.list': 'nobroker:9092',                    
        'socket.keepalive.enable': true,
        'enable.auto.commit': true,
        'queue.buffering.max.ms': 1,
        'fetch.min.bytes': 1,
        'fetch.wait.max.ms': 1,    
        'fetch.error.backoff.ms': 100,
        'api.version.request': true
    }
}

describe('Kafka Consumer Node', () => {
    const env = process.env;
    beforeEach((done) => {
        jestFw.clearAllMocks();
        helper.startServer(done);
        process.env = { ...env }
    });

    afterEach((done) => {
        helper.unload();
        helper.stopServer(done);
        process.env = env;
    });

    it('Should show red status if no broker configured', async () => {
        await helper.load([KafkaInNode], [{id: 'test', name: 'testIn', type: 'rdkafka-secure-in', topic: 'test_topic'}]);
        const n1 = await helper.getNode('test');
        n1.status.should.be.calledWithExactly({fill: 'red', shape: 'ring', text: 'missing broker configuration'});        
    });

    it('Should show red status if no topic configured', async () => {
        const flow = getTestFlow('String', 'String', 'String');
        delete flow.at(1).topic;
        await helper.load([KafkaInNode, KafkaBroker], flow);
        const n1 = await helper.getNode('test');
        n1.status.should.be.calledWithExactly({fill: 'red', shape: 'ring', text: 'missing topic'});
    });

    it('Should show red status if topic is empty string', async () => {
        const flow = getTestFlow('String', 'String', 'String');
        flow.at(1).topic = "";
        await helper.load([KafkaInNode, KafkaBroker], flow);
        const n1 = await helper.getNode('test');
        n1.status.should.be.calledWithExactly({fill: 'red', shape: 'ring', text: 'missing topic'});
    });

    it('Should handle optional broker parameters', async () => {
        const flow = getTestFlow('String', 'String', 'String');
        const consumerOptions = getDefaultOptions();
        flow.at(0)['security_protocol'] = 'sasl';
        consumerOptions['security.protocol'] = 'sasl';
        flow.at(0)['sasl_mechanism'] = 'sasl-mechanism';
        consumerOptions['sasl.mechanisms'] = 'sasl-mechanism';
        flow.at(0)['ssl_ca_location'] = 'location';
        consumerOptions['ssl.ca.location'] = 'location';
        flow.at(0)['enable_ssl_certificate_verification'] = true;
        consumerOptions['enable.ssl.certificate.verification'] = true;
        flow.at(1)['cgroup'] = 'consumerGroup1';
        flow.at(1)['cgroupType'] = 'str';
        consumerOptions['group.id'] = 'consumerGroup1';
        consumerOptions['sasl.password'] = 'password';
        consumerOptions['sasl.username'] = 'username';
        await helper.load([KafkaInNode, KafkaBroker], flow, {testbroker: {sasl_username: 'username', sasl_password: 'password'}});
        const n1 = await helper.getNode('test');
        expect(mockConsumer).toHaveBeenCalledWith(consumerOptions);
        expect(n1.log.calledTwice).toBe(true);
        expect(n1.log.firstCall.args[0]).toContain('security.protocol');
        expect(n1.log.firstCall.args[0]).not.toContain('password');
        expect(n1.log.firstCall.args[0]).not.toContain('username');
    });

    it('Should replace empty consumer group id', async () => {
        const flow = getTestFlow('String', 'String', 'String');
        const consumerOptions = getDefaultOptions();
        flow.at(1)['cgroup'] = '';
        flow.at(1)['cgroupType'] = 'str';
        consumerOptions['sasl.password'] = 'password';
        consumerOptions['sasl.username'] = 'username';
        await helper.load([KafkaInNode, KafkaBroker], flow, {testbroker: {sasl_username: 'username', sasl_password: 'password'}});
        const n1 = await helper.getNode('test');
        expect(mockConsumer).toHaveBeenCalledWith(consumerOptions);
        expect(n1.log.calledTwice).toBe(true);
        expect(n1.log.firstCall.args[0]).not.toContain('security.protocol');
        expect(n1.log.firstCall.args[0]).not.toContain('password');
        expect(n1.log.firstCall.args[0]).not.toContain('username');
    });

    it('Should use consumer group id from env', async () => {
        const flow = getTestFlow('String', 'String', 'String');
        const consumerOptions = getDefaultOptions();
        flow.at(1)['cgroup'] = 'CGROUP';
        flow.at(1)['cgroupType'] = 'env';
        process.env.CGROUP = 'testGroup';
        consumerOptions['group.id'] = 'testGroup';
        consumerOptions['sasl.password'] = 'password';
        consumerOptions['sasl.username'] = 'username';
        await helper.load([KafkaInNode, KafkaBroker], flow, {testbroker: {sasl_username: 'username', sasl_password: 'password'}});
        const n1 = await helper.getNode('test');
        expect(mockConsumer).toHaveBeenCalledWith(consumerOptions);
        expect(n1.log.calledTwice).toBe(true);
        expect(n1.log.firstCall.args[0]).not.toContain('security.protocol');
        expect(n1.log.firstCall.args[0]).not.toContain('password');
        expect(n1.log.firstCall.args[0]).not.toContain('username');
    });


    it('Should consume from Kafka if inputs are ok', async () => {
        const flow = getTestFlow('String', 'String', 'String');
        await helper.load([KafkaInNode, KafkaBroker], flow);
        const n1 = await helper.getNode('test');
        expect(mockConsumerListeners.ready).toBeDefined();
        expect(mockConsumerListeners.data).toBeDefined();
        mockConsumerListeners.ready();
        expect(mockConsumerInstance.connect).toHaveBeenCalledTimes(1);
        expect(mockConsumerInstance.subscribe).toHaveBeenCalledTimes(1);
        expect(mockConsumerInstance.consume).toHaveBeenCalledTimes(1);
        expect(mockConsumer).toHaveBeenCalledWith(getDefaultOptions());
    });

    it('Should handle Kafka event.error', async () => {
        const flow = getTestFlow('String', 'String', 'String');
        const errorMock = jestFw.fn();
        await helper.load([KafkaInNode, KafkaBroker], flow);
        const n1 = await helper.getNode('test');
        n1.error = errorMock;
        expect(mockConsumerListeners.ready).toBeDefined();
        expect(mockConsumerListeners['event.error']).toBeDefined();
        mockConsumerListeners.ready();
        mockConsumerListeners['event.error']({message: 'error from kafka', code: 1});
        expect(mockConsumerInstance.connect).toHaveBeenCalledTimes(1);
        expect(mockConsumerInstance.subscribe).toHaveBeenCalledTimes(1);
        expect(mockConsumerInstance.consume).toHaveBeenCalledTimes(1);
        n1.status.should.be.calledWithExactly({fill: 'red', shape: 'ring', text: 'error from kafka'});
        expect(errorMock).toHaveBeenCalledTimes(1);
    });

    it('Should handle Kafka connection.failure', async () => {
        const flow = getTestFlow('String', 'String', 'String');
        const errorMock = jestFw.fn();
        await helper.load([KafkaInNode, KafkaBroker], flow);
        const n1 = await helper.getNode('test');
        n1.error = errorMock;
        expect(mockConsumerListeners.ready).toBeDefined();
        expect(mockConsumerListeners['connection.failure']).toBeDefined();
        mockConsumerListeners.ready();
        mockConsumerListeners['connection.failure']({message: 'error from kafka', code: 1});
        expect(mockConsumerInstance.connect).toHaveBeenCalledTimes(1);
        expect(mockConsumerInstance.subscribe).toHaveBeenCalledTimes(1);
        expect(mockConsumerInstance.consume).toHaveBeenCalledTimes(1);
        n1.status.should.be.calledWithExactly({fill: 'red', shape: 'ring', text: 'error from kafka'});
        expect(errorMock).toHaveBeenCalledTimes(1);
    });

    it('Should transform null payload to string for payloadType === "String"', async () => {
        const flow = getTestFlow('String', 'String', 'String');
        const testData = getTestData();
        await helper.load([KafkaInNode, KafkaBroker], flow);
        const n1 = await helper.getNode('test');
        n1.send = jestFw.fn((args) => {
            const transformedData = getTestData();
            delete transformedData.value;
            transformedData.payload = "";
            expect(args).toEqual(transformedData);
        })
        expect(mockConsumerListeners.ready).toBeDefined();
        expect(mockConsumerListeners.data).toBeDefined();
        mockConsumerListeners.ready();
        mockConsumerListeners.data(testData);
    });

    it('Should transform non-null payload to string for payloadType === "String"', async () => {
        const flow = getTestFlow('String', 'String', 'String');
        const testData = getTestData();
        testData.value = Buffer.from('testData');
        await helper.load([KafkaInNode, KafkaBroker], flow);
        const n1 = await helper.getNode('test');
        n1.send = jestFw.fn((args) => {
            const transformedData = getTestData();
            delete transformedData.value;
            transformedData.payload = 'testData';
            expect(args).toEqual(transformedData);
        })
        expect(mockConsumerListeners.ready).toBeDefined();
        expect(mockConsumerListeners.data).toBeDefined();
        mockConsumerListeners.ready();
        mockConsumerListeners.data(testData);
    });

    it('Should transform null payload to Buffer for payloadType === "Buffer"', async () => {
        const flow = getTestFlow('String', 'String', 'Buffer');
        const testData = getTestData();
        await helper.load([KafkaInNode, KafkaBroker], flow);
        const n1 = await helper.getNode('test');
        n1.send = jestFw.fn((args) => {
            const transformedData = getTestData();
            delete transformedData.value;
            transformedData.payload = Buffer.from("");
            expect(args).toEqual(transformedData);
        })
        expect(mockConsumerListeners.ready).toBeDefined();
        expect(mockConsumerListeners.data).toBeDefined();
        mockConsumerListeners.ready();
        mockConsumerListeners.data(testData);
    });

    it('Should return non-null payload for payloadType === "Buffer"', async () => {
        const flow = getTestFlow('String', 'String', 'Buffer');
        const testData = getTestData();
        testData.value = Buffer.from('testData');
        await helper.load([KafkaInNode, KafkaBroker], flow);
        const n1 = await helper.getNode('test');
        n1.send = jestFw.fn((args) => {
            const transformedData = {...testData};
            transformedData.payload = transformedData.value;
            delete transformedData.value;
            expect(args).toEqual(transformedData);
        })
        expect(mockConsumerListeners.ready).toBeDefined();
        expect(mockConsumerListeners.data).toBeDefined();
        mockConsumerListeners.ready();
        mockConsumerListeners.data(testData);
    });

    it('Should transform key to Buffer for keyType === "Buffer"', async () => {
        const flow = getTestFlow('String', 'Buffer', 'Buffer');
        const testData = getTestData();
        testData.key = 'key';
        await helper.load([KafkaInNode, KafkaBroker], flow);
        const n1 = await helper.getNode('test');
        n1.send = jestFw.fn((args) => {
            const transformedData = getTestData();
            delete transformedData.value;
            transformedData.payload = Buffer.from("");
            transformedData.key = Buffer.from("key");
            expect(args).toEqual(transformedData);
        })
        expect(mockConsumerListeners.ready).toBeDefined();
        expect(mockConsumerListeners.data).toBeDefined();
        mockConsumerListeners.ready();
        mockConsumerListeners.data(testData);
    });

    it('Should transform key to String for keyType === "String"', async () => {
        const flow = getTestFlow('String', 'String', 'Buffer');
        const testData = getTestData();
        testData.key = Buffer.from('key');
        await helper.load([KafkaInNode, KafkaBroker], flow);
        const n1 = await helper.getNode('test');
        n1.send = jestFw.fn((args) => {
            const transformedData = getTestData();
            delete transformedData.value;
            transformedData.payload = Buffer.from("");
            transformedData.key = "key";
            expect(args).toEqual(transformedData);
        })
        expect(mockConsumerListeners.ready).toBeDefined();
        expect(mockConsumerListeners.data).toBeDefined();
        mockConsumerListeners.ready();
        mockConsumerListeners.data(testData);
    });

    it('Should leave key unchanged for keyType === "Unchanged"', async () => {
        const flow = getTestFlow('String', 'Unchanged', 'String');
        const testData = getTestData();
        testData.key = 'unchanged';
        await helper.load([KafkaInNode, KafkaBroker], flow);
        const n1 = await helper.getNode('test');
        n1.send = jestFw.fn((args) => {
            const transformedData = {...testData};
            delete transformedData.value;
            transformedData.payload = "";
            expect(args).toEqual(transformedData);
        })
        expect(mockConsumerListeners.ready).toBeDefined();
        expect(mockConsumerListeners.data).toBeDefined();
        mockConsumerListeners.ready();
        mockConsumerListeners.data(testData);
    });

    it('Should transform headers to string for headerType === "String"', async () => {
        const flow = getTestFlow('String', 'Unchanged', 'String');
        const testData = getTestData();
        testData.headers = [{header1: Buffer.from('testHeader')}];
        await helper.load([KafkaInNode, KafkaBroker], flow);
        const n1 = await helper.getNode('test');
        n1.send = jestFw.fn((args) => {
            const transformedData = {...testData};
            delete transformedData.value;
            transformedData.payload = "";
            transformedData.headers = [{header1: 'testHeader'}];
            expect(args).toEqual(transformedData);
        })
        expect(mockConsumerListeners.ready).toBeDefined();
        expect(mockConsumerListeners.data).toBeDefined();
        mockConsumerListeners.ready();
        mockConsumerListeners.data(testData);
    });

    it('Should transform headers to Buffer for headerType === "Buffer"', async () => {
        const flow = getTestFlow('Buffer', 'Unchanged', 'String');
        const testData = getTestData();
        testData.headers = [{header1: 'testHeader'}];
        await helper.load([KafkaInNode, KafkaBroker], flow);
        const n1 = await helper.getNode('test');
        n1.send = jestFw.fn((args) => {
            const transformedData = {...testData};
            delete transformedData.value;
            transformedData.payload = "";
            transformedData.headers = [{header1: Buffer.from('testHeader')}];
            expect(args).toEqual(transformedData);
        })
        expect(mockConsumerListeners.ready).toBeDefined();
        expect(mockConsumerListeners.data).toBeDefined();
        mockConsumerListeners.ready();
        mockConsumerListeners.data(testData);
    });

    it('Should return buffer-headers as Buffer for headerType === "Buffer"', async () => {
        const flow = getTestFlow('Buffer', 'Unchanged', 'String');
        const testData = getTestData();
        testData.headers = [{header1: Buffer.from('testHeader')}];
        await helper.load([KafkaInNode, KafkaBroker], flow);
        const n1 = await helper.getNode('test');
        n1.send = jestFw.fn((args) => {
            const transformedData = {...testData};
            delete transformedData.value;
            transformedData.payload = "";
            expect(args).toEqual(transformedData);
        })
        expect(mockConsumerListeners.ready).toBeDefined();
        expect(mockConsumerListeners.data).toBeDefined();
        mockConsumerListeners.ready();
        mockConsumerListeners.data(testData);
    });

    it('Should return string-headers as string for headerType === "String"', async () => {
        const flow = getTestFlow('String', 'Unchanged', 'String');
        const testData = getTestData();
        testData.headers = [{header1: 'testHeader'}];
        await helper.load([KafkaInNode, KafkaBroker], flow);
        const n1 = await helper.getNode('test');
        n1.send = jestFw.fn((args) => {
            const transformedData = {...testData};
            delete transformedData.value;
            transformedData.payload = "";
            expect(args).toEqual(transformedData);
        })
        expect(mockConsumerListeners.ready).toBeDefined();
        expect(mockConsumerListeners.data).toBeDefined();
        mockConsumerListeners.ready();
        mockConsumerListeners.data(testData);
    });

    it('Should return string-headers unchanged for headerType === "Unchanged"', async () => {
        const flow = getTestFlow('Unchanged', 'Unchanged', 'String');
        const testData = getTestData();
        testData.headers = [{header1: 'testHeader'}];
        await helper.load([KafkaInNode, KafkaBroker], flow);
        const n1 = await helper.getNode('test');
        n1.send = jestFw.fn((args) => {
            const transformedData = {...testData};
            delete transformedData.value;
            transformedData.payload = "";
            expect(args).toEqual(transformedData);
        })
        expect(mockConsumerListeners.ready).toBeDefined();
        expect(mockConsumerListeners.data).toBeDefined();
        mockConsumerListeners.ready();
        mockConsumerListeners.data(testData);
    });



});