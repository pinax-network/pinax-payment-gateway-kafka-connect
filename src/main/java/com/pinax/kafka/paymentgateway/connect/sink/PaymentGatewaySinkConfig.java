package com.pinax.kafka.paymentgateway.connect.sink;

import org.apache.kafka.common.config.ConfigDef;

public class PaymentGatewaySinkConfig {
        public static final String ENDPOINT = "endpoint";
        public static final String TOKEN = "token";
        public static final String BATCH_SIZE = "batchSize";

        public static final ConfigDef CONFIG_DEF = new ConfigDef()
                        .define(ENDPOINT,
                                        ConfigDef.Type.STRING,
                                        "http://localhost:8080",
                                        new PaymentGatewaySinkConfigValidator(),
                                        ConfigDef.Importance.HIGH,
                                        "Payment Gateway Endpoint")
                        .define(TOKEN,
                                        ConfigDef.Type.STRING,
                                        "token",
                                        new PaymentGatewaySinkConfigValidator(),
                                        ConfigDef.Importance.HIGH,
                                        "Payment Gateway Token")
                        .define(BATCH_SIZE,
                                        ConfigDef.Type.INT,
                                        100,
                                        new PaymentGatewaySinkConfigValidator(),
                                        ConfigDef.Importance.HIGH,
                                        "Events Batch Size");
}
