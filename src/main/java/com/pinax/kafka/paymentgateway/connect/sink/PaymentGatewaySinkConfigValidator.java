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
        // `usePlaintext` setting, not by a scheme.
        String endpoint = (String) value;
        int sep = endpoint.lastIndexOf(':');
        if (sep <= 0 || sep == endpoint.length() - 1) {
            throw new ConfigException(name, value, "Endpoint must be of the form host:port");
        }

        String host = endpoint.substring(0, sep);
        if (host.contains("/")) {
            throw new ConfigException(name, value, "Endpoint must be host:port, without a scheme or path");
        }

        final int port;
        try {
            port = Integer.parseInt(endpoint.substring(sep + 1));
        } catch (NumberFormatException e) {
            throw new ConfigException(name, value, "Endpoint port must be a number");
        }
        if (port < 1 || port > 65535) {
            throw new ConfigException(name, value, "Endpoint port must be between 1 and 65535");
        }
    }
}
