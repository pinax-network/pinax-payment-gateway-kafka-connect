package com.pinax.kafka.paymentgateway.connect.sink;

import org.apache.kafka.common.config.ConfigDef;

public class PaymentGatewaySinkConfig {
        public static final String ENDPOINT = "endpoint";
        public static final String TOKEN = "token";
        public static final String USE_PLAINTEXT = "usePlaintext";
        public static final String USE_INSECURE = "useInsecure";
        // Reserved: size-based batching is disabled until StreamingFast supports
        // it, so this value is validated but not currently used by the client.
        public static final String BATCH_SIZE = "batchSize";

        public static final ConfigDef CONFIG_DEF = new ConfigDef()
                        .define(ENDPOINT,
                                        ConfigDef.Type.STRING,
                                        "localhost:8080",
                                        new PaymentGatewaySinkConfigValidator(),
                                        ConfigDef.Importance.HIGH,
                                        "Payment Gateway gRPC endpoint as host:port (e.g. abp.thegraph.market:443)")
                        .define(TOKEN,
                                        ConfigDef.Type.STRING,
                                        "token",
                                        new ConfigDef.NonEmptyString(),
                                        ConfigDef.Importance.HIGH,
                                        "Payment Gateway Token")
                        .define(USE_PLAINTEXT,
                                        ConfigDef.Type.BOOLEAN,
                                        false,
                                        ConfigDef.Importance.MEDIUM,
                                        "Use a plaintext (non-TLS) gRPC connection. Defaults to false (TLS enabled). "
                                                        + "Mutually exclusive with useInsecure")
                        .define(USE_INSECURE,
                                        ConfigDef.Type.BOOLEAN,
                                        false,
                                        ConfigDef.Importance.MEDIUM,
                                        "Use TLS but skip server certificate verification. Defaults to false. "
                                                        + "Mutually exclusive with usePlaintext")
                        .define(BATCH_SIZE,
                                        ConfigDef.Type.INT,
                                        100,
                                        new PaymentGatewaySinkConfigValidator(),
                                        ConfigDef.Importance.MEDIUM,
                                        "Events Batch Size");
}
