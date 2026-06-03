package com.pinax.kafka.paymentgateway.connect.sink;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PaymentGatewaySinkConnector extends SinkConnector {

    protected static final String VERSION = "0.0.1";

    private final Logger logger = LoggerFactory.getLogger(PaymentGatewaySinkConnector.class);

    private Map<String, String> configs = null;

    @Override
    public String version() {
        return VERSION;
    }

    @Override
    public void start(Map<String, String> configMap) {
        // Do not log the config map: it contains the bearer token.
        logger.info("Starting PaymentGateway sink connector");
        // Defensive copy so the connector does not retain a reference to a map
        // owned by the caller (and hand it out again in taskConfigs).
        configs = new HashMap<>(configMap);
    }

    @Override
    public ConfigDef config() {
        return PaymentGatewaySinkConfig.CONFIG_DEF;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return PaymentGatewaySinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxConfigs) {
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        for (int task = 0; task < maxConfigs; task++) {
            taskConfigs.add(new HashMap<>(configs));
        }
        return taskConfigs;
    }

    @Override
    public void stop() {
        logger.info("Stopping connector");
    }
}