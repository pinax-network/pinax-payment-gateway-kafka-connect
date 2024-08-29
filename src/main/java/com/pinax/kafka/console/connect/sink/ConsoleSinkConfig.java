package com.pinax.kafka.console.connect.sink;

import org.apache.kafka.common.config.ConfigDef;

public class ConsoleSinkConfig {
        public static final String PREFIX = "prefix";

        public static final ConfigDef CONFIG_DEF = new ConfigDef()
                        .define(PREFIX,
                                        ConfigDef.Type.STRING,
                                        "prefix-",
                                        ConfigDef.Importance.LOW,
                                        "Prefix that will be added to the records in the output");
}
