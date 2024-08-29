package com.pinax.kafka.console.connect.sink;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsoleSinkConnector extends SinkConnector {

    protected static final String VERSION = "0.0.1";

    private final Logger logger = LoggerFactory.getLogger(ConsoleSinkConnector.class);

    private Map<String, String> configs = null;

    @Override
    public String version() {
        return VERSION;
    }

    @Override
    public void start(Map<String, String> configMap) {
        logger.info("Starting connector {}", configMap);
        configs = configMap;
    }

    @Override
    public ConfigDef config() {
        return ConsoleSinkConfig.CONFIG_DEF;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return ConsoleSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxConfigs) {
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        for (int task = 0; task < maxConfigs; task++) {
            taskConfigs.add(configs);
        }
        return taskConfigs;
    }

    @Override
    public void stop() {
        logger.info("Stopping connector");
    }
}
