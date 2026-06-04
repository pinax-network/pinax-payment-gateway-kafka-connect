package com.pinax.kafka.paymentgateway.connect.sink;

import java.util.Collection;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PaymentGatewaySinkTask extends SinkTask {
    private final Logger logger = LoggerFactory.getLogger(PaymentGatewaySinkTask.class);

    private PaymentGatewayClient client;

    @Override
    public void start(Map<String, String> properties) {
        AbstractConfig config = new AbstractConfig(PaymentGatewaySinkConfig.CONFIG_DEF, properties);

        // Do not log the properties map (it contains the bearer token) or the raw
        // endpoint; the client logs the normalised scheme://host:port target.
        logger.info("Starting PaymentGateway sink task");

        // Create the client used to report usage to the Payment Gateway.
        client = new PaymentGatewayClient(
                config.getString(PaymentGatewaySinkConfig.ENDPOINT),
                config.getString(PaymentGatewaySinkConfig.TOKEN));

        client.start();
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            return;
        }

        try {
            logger.info("Reporting {} events to the PaymentGateway", records.size());
            client.report(records);
            logger.info("Successfully reported {} events to the PaymentGateway", records.size());
        } catch (Exception e) {
            // Drop the (possibly broken) connection so the next batch reconnects
            // from a clean state, then let Connect handle the exception: a
            // RetriableException is retried, anything else fails the task.
            client.stop();
            throw e;
        }
    }

    @Override
    public void stop() {
        logger.info("Stopping PaymentGateway sink task");
        if (client != null) {
            client.stop();
        }
    }

    @Override
    public String version() {
        return PaymentGatewaySinkConnector.VERSION;
    }
}
