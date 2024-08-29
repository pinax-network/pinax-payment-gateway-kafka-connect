package com.pinax.kafka.console.connect.sink;

import java.util.Collection;
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsoleSinkTask extends SinkTask {
    private final Logger logger = LoggerFactory.getLogger(ConsoleSinkConnector.class);

    private String prefix;

    @Override
    public void start(Map<String, String> properties) {
        logger.info("Starting Console sink task {}", properties);

        AbstractConfig config = new AbstractConfig(ConsoleSinkConfig.CONFIG_DEF, properties);

        this.prefix = config.getString(ConsoleSinkConfig.PREFIX);
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.size() > 0) {
            logger.debug("Received records from Connect");
        }

        String key = null;
        String value = null;

        try {

            for (SinkRecord record : records) {
                logger.debug("Processing record: {}", record);
                key = record.key() == null ? "" : record.key().toString();
                value = record.value() == null ? "" : record.value().toString();

                String recordValue = prefix + key + "," + value + "\n";
                System.out.println(recordValue);

                logger.debug("Record written to Console: key={}, value={}", key, value);
            }

        } catch (Exception e) {
            final String message = "Failed to write record to Console: key=" + key + ", value=" + value;
            logger.error(message, e);
            throw new DataException(message, e);
        }
    }

    @Override
    public void stop() {
        logger.info("Stopping Console sink task");
    }

    @Override
    public String version() {
        return ConsoleSinkConnector.VERSION;
    }
}
