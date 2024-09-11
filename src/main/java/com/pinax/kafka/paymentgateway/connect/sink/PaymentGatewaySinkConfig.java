package com.pinax.kafka.paymentgateway.connect.sink;

import org.apache.kafka.common.config.ConfigDef;

public class PaymentGatewaySinkConfig {
        public static final String ENDPOINT = "endpoint";
        public static final String TOKEN = "token";
        public static final String BUFFER_SIZE = "bufferSize";
        public static final String BACTH_INTERVAL = "batchInterval";

        public static final ConfigDef CONFIG_DEF = new ConfigDef()
                        .define(ENDPOINT,
                                        ConfigDef.Type.STRING,
                                        "http://localhost:8080",
                                        ConfigDef.Importance.HIGH,
                                        "Payment Gateway Endpoint")
                        .define(TOKEN,
                                        ConfigDef.Type.STRING,
                                        "",
                                        new PaymentGatewaySinkConfigValidator(),
                                        ConfigDef.Importance.HIGH,
                                        "Payment Gateway Token")
                        .define(BUFFER_SIZE,
                                        ConfigDef.Type.INT,
                                        100,
                                        ConfigDef.Importance.HIGH,
                                        "Buffer Size")
                        .define(BACTH_INTERVAL,
                                        ConfigDef.Type.LONG,
                                        1000,
                                        ConfigDef.Importance.HIGH,
                                        "Batch Interval");
}
