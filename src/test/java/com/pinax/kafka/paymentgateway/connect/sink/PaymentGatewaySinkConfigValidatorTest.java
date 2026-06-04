package com.pinax.kafka.paymentgateway.connect.sink;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

class PaymentGatewaySinkConfigValidatorTest {

    private final PaymentGatewaySinkConfigValidator validator = new PaymentGatewaySinkConfigValidator();

    @Test
    void endpoint_validHostPort_passes() {
        assertDoesNotThrow(() -> validator.ensureValid(PaymentGatewaySinkConfig.ENDPOINT, "abp.thegraph.market:443"));
        assertDoesNotThrow(() -> validator.ensureValid(PaymentGatewaySinkConfig.ENDPOINT, "localhost:8080"));
    }

    @Test
    void endpoint_missingPort_throws() {
        assertThrows(ConfigException.class,
                () -> validator.ensureValid(PaymentGatewaySinkConfig.ENDPOINT, "gateway.test"));
    }

    @Test
    void endpoint_emptyPort_throws() {
        assertThrows(ConfigException.class,
                () -> validator.ensureValid(PaymentGatewaySinkConfig.ENDPOINT, "gateway.test:"));
    }

    @Test
    void endpoint_missingHost_throws() {
        assertThrows(ConfigException.class,
                () -> validator.ensureValid(PaymentGatewaySinkConfig.ENDPOINT, ":443"));
    }

    @Test
    void endpoint_nonNumericPort_throws() {
        assertThrows(ConfigException.class,
                () -> validator.ensureValid(PaymentGatewaySinkConfig.ENDPOINT, "gateway.test:https"));
    }

    @Test
    void endpoint_portOutOfRange_throws() {
        assertThrows(ConfigException.class,
                () -> validator.ensureValid(PaymentGatewaySinkConfig.ENDPOINT, "gateway.test:0"));
        assertThrows(ConfigException.class,
                () -> validator.ensureValid(PaymentGatewaySinkConfig.ENDPOINT, "gateway.test:70000"));
    }

    @Test
    void endpoint_withScheme_throws() {
        // A pasted URL (scheme/path) is a common mistake — reject it clearly.
        assertThrows(ConfigException.class,
                () -> validator.ensureValid(PaymentGatewaySinkConfig.ENDPOINT, "https://gateway.test:443"));
    }

    @Test
    void batchSize_positive_passes() {
        assertDoesNotThrow(() -> validator.ensureValid(PaymentGatewaySinkConfig.BATCH_SIZE, 100));
    }

    @Test
    void batchSize_zeroOrNegative_throws() {
        assertThrows(ConfigException.class,
                () -> validator.ensureValid(PaymentGatewaySinkConfig.BATCH_SIZE, 0));
        assertThrows(ConfigException.class,
                () -> validator.ensureValid(PaymentGatewaySinkConfig.BATCH_SIZE, -1));
    }

    @Test
    void unrelatedKey_isIgnored() {
        assertDoesNotThrow(() -> validator.ensureValid("some.other.key", "whatever"));
    }
}
