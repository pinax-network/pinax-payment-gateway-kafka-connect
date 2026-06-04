package com.pinax.kafka.paymentgateway.connect.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

class PaymentGatewaySinkConfigTest {

    private static AbstractConfig parse(Map<String, String> props) {
        return new AbstractConfig(PaymentGatewaySinkConfig.CONFIG_DEF, props);
    }

    @Test
    void parsesValidConfig() {
        Map<String, String> props = new HashMap<>();
        props.put(PaymentGatewaySinkConfig.ENDPOINT, "gateway.test:443");
        props.put(PaymentGatewaySinkConfig.TOKEN, "secret");
        props.put(PaymentGatewaySinkConfig.USE_PLAINTEXT, "true");
        props.put(PaymentGatewaySinkConfig.BATCH_SIZE, "250");

        AbstractConfig config = parse(props);

        assertEquals("gateway.test:443", config.getString(PaymentGatewaySinkConfig.ENDPOINT));
        assertEquals("secret", config.getString(PaymentGatewaySinkConfig.TOKEN));
        assertEquals(true, config.getBoolean(PaymentGatewaySinkConfig.USE_PLAINTEXT));
        assertEquals(250, config.getInt(PaymentGatewaySinkConfig.BATCH_SIZE));
    }

    @Test
    void appliesDefaults() {
        AbstractConfig config = parse(new HashMap<>());

        assertEquals("localhost:8080", config.getString(PaymentGatewaySinkConfig.ENDPOINT));
        assertEquals("token", config.getString(PaymentGatewaySinkConfig.TOKEN));
        assertEquals(false, config.getBoolean(PaymentGatewaySinkConfig.USE_PLAINTEXT));
        assertEquals(false, config.getBoolean(PaymentGatewaySinkConfig.USE_INSECURE));
        assertEquals(100, config.getInt(PaymentGatewaySinkConfig.BATCH_SIZE));
    }

    @Test
    void invalidEndpoint_throwsAtParse() {
        Map<String, String> props = new HashMap<>();
        props.put(PaymentGatewaySinkConfig.ENDPOINT, "gateway.test"); // no port
        assertThrows(ConfigException.class, () -> parse(props));
    }

    @Test
    void emptyToken_throwsAtParse() {
        Map<String, String> props = new HashMap<>();
        props.put(PaymentGatewaySinkConfig.TOKEN, "");
        assertThrows(ConfigException.class, () -> parse(props));
    }
}
