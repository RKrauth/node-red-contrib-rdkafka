const helper = require('node-red-node-test-helper');
const KafkaBroker = require('../src/rdkafka-secure-broker');
const {beforeEach, afterEach, it, describe, expect, jest: jestFw} = require('@jest/globals');
helper.init(require.resolve('node-red'));

const testFlow = [{
    id: 'test',
    type: 'rdkafka-secure-broker',
    name: 'broker',
    broker: 'nobroker:9092',
    clientid: 'clientId',
}];

describe('Broker', () => {
    beforeEach((done) => {
        jestFw.clearAllMocks();
        helper.startServer(done);
    });

    afterEach((done) => {
        helper.unload();
        helper.stopServer(done);
    });

    it('Should return Kafka features via http.get', async () => {
        await helper.load([KafkaBroker], testFlow);
        await helper.getNode('test');
        const res = await helper.request().get('/rdkafka');
        expect(res.status).toBe(200);
        expect(res.body).toHaveProperty('features');        
    });
})