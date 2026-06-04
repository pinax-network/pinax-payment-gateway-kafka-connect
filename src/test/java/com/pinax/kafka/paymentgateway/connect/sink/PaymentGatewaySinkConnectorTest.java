package com.pinax.kafka.paymentgateway.connect.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

class PaymentGatewaySinkConnectorTest {

    private final PaymentGatewaySinkConnector connector = new PaymentGatewaySinkConnector();

    @Test
    void version_isReported() {
        assertEquals(PaymentGatewaySinkConnector.VERSION, connector.version());
    }

    @Test
    void config_returnsSharedConfigDef() {
        assertSame(PaymentGatewaySinkConfig.CONFIG_DEF, connector.config());
    }

    @Test
    void taskClass_isPaymentGatewaySinkTask() {
        assertEquals(PaymentGatewaySinkTask.class, connector.taskClass());
    }

    @Test
    void taskConfigs_replicatesStartConfigForEachTask() {
        Map<String, String> props = new HashMap<>();
        props.put(PaymentGatewaySinkConfig.ENDPOINT, "gateway.test:443");
        props.put(PaymentGatewaySinkConfig.TOKEN, "secret");
        connector.start(props);

        List<Map<String, String>> taskConfigs = connector.taskConfigs(3);

        assertEquals(3, taskConfigs.size());
        for (Map<String, String> taskConfig : taskConfigs) {
            assertEquals(props, taskConfig);
        }
    }
}
