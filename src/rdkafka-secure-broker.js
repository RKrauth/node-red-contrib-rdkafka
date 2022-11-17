"use strict";
module.exports = function(RED) {
    const Kafka = require('node-rdkafka');

    function KafkaBrokerNode(n) {
        RED.nodes.createNode(this, n);
        this.broker = n.broker;
        this.clientid = n.clientid;
        this.security_protocol = n.security_protocol;
        this.sasl_mechanism = n.sasl_mechanism;
        this.ssl_ca_location = n.ssl_ca_location;
        this.enable_ssl_certificate_verification = n.enable_ssl_certificate_verification;

        this.ssl_support_enabled = Kafka.features.indexOf('ssl') >= 0;
    }
    RED.nodes.registerType("rdkafka-secure-broker", KafkaBrokerNode, {
        credentials: {
            sasl_username: { type: "text" },
            sasl_password: { type: "password" }
        }
    });

    // allow the UI to query for the features that are enabled in the rdkafka library
    RED.httpAdmin.get("/rdkafka", RED.auth.needsPermission('rdkafka.read'), function(_req,res) {
        res.json({ features : Kafka.features });
    });
}