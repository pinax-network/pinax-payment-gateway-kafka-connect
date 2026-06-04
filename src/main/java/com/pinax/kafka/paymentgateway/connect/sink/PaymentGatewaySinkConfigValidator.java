package com.pinax.kafka.paymentgateway.connect.sink;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public class PaymentGatewaySinkConfigValidator implements ConfigDef.Validator {

    @Override
    public void ensureValid(String name, Object value) {
        if (PaymentGatewaySinkConfig.ENDPOINT.equals(name)) {
            validateEndpoint(name, value);
        } else if (PaymentGatewaySinkConfig.BATCH_SIZE.equals(name)) {
            if ((int) value <= 0) {
                throw new ConfigException(name, value, "Batch size must be greater than 0");
            }
        }
    }

    private void validateEndpoint(String name, Object value) {
        // This is a gRPC endpoint, not a URL: it must be a bare "host:port"
        // (e.g. abp.thegraph.market:443). TLS is controlled by the separate
        // `usePlaintext`/`useInsecure` settings, not by a scheme. Parsing goes
        // through Endpoint.parse so config validation and the client's host/port
        // can never disagree.
        try {
            Endpoint.parse((String) value);
        } catch (ConfigException e) {
            // Re-associate the failure with this config key for a clearer message.
            throw new ConfigException(name, value, e.getMessage());
        }
    }
}
