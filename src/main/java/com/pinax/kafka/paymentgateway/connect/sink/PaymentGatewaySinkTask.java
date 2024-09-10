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

    @Override
    public void start(Map<String, String> properties) {
        logger.info("Starting PaymentGateway sink task {}", properties);

        AbstractConfig config = new AbstractConfig(PaymentGatewaySinkConfig.CONFIG_DEF, properties);

        // TODO: Implement the actual sink initialization logic here
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.size() > 0) {
            logger.debug("Received records from Connect");
        }

        // TODO: Implement the actual sink logic here
    }

    @Override
    public void stop() {
        logger.info("Stopping PaymentGateway sink task");

        // TODO: Implement the actual sink cleanup logic here
    }

    @Override
    public String version() {
        return PaymentGatewaySinkConnector.VERSION;
    }
}