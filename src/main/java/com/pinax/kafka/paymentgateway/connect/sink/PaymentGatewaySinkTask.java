package com.pinax.kafka.paymentgateway.connect.sink;

import java.util.Collection;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PaymentGatewaySinkTask extends SinkTask {
    private final Logger logger = LoggerFactory.getLogger(PaymentGatewaySinkConnector.class);

    private PaymentGatewayClient client;

    @Override
    public void start(Map<String, String> properties) {
        logger.info("Starting PaymentGateway sink task {}", properties);

        AbstractConfig config = new AbstractConfig(PaymentGatewaySinkConfig.CONFIG_DEF, properties);

        // Create a client to connect to the Payment Gateway
        client = new PaymentGatewayClient(
                config.getString(PaymentGatewaySinkConfig.ENDPOINT),
                config.getString(PaymentGatewaySinkConfig.TOKEN),
                config.getInt(PaymentGatewaySinkConfig.BATCH_SIZE));

        // Start the client
        client.start();
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        try {
            // Report the records to the Payment Gateway
            client.report(records);
        } catch (Exception e) {
            // Stop the client
            client.stop();

            // Restart the client
            client.start();

            // Propagate the exception
            throw e;
        }
    }

    @Override
    public void stop() {
        logger.info("Stopping PaymentGateway sink task");

        // Stop the client
        client.stop();
    }

    @Override
    public String version() {
        return PaymentGatewaySinkConnector.VERSION;
    }
}