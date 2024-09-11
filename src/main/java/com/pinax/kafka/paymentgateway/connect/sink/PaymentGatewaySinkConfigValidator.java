package com.pinax.kafka.paymentgateway.connect.sink;

import java.net.*;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public class PaymentGatewaySinkConfigValidator implements ConfigDef.Validator {

    @SuppressWarnings("unchecked")
    public void ensureValid(String name, Object value) {
        if (name.equals(PaymentGatewaySinkConfig.ENDPOINT)) {
            try {
                URI uri = new URI((String) value);
                String host = uri.getHost();
                if (host == null || host.isEmpty()) {
                    throw new ConfigException(name, value, "Not a valid URL");
                }

                int port = uri.getPort();
                if (port == -1) {
                    throw new ConfigException(name, value, "Not a valid URL");
                }

                // String scheme = uri.getScheme();
                // if (scheme == null || !scheme.equals("http")) { // TODO: handle different
                // schemes
                // throw new ConfigException(name, value, "Not a valid URL");
                // }
            } catch (Exception e) {
                throw new ConfigException(name, value, "Not a valid URL");
            }
        } else if (name.equals(PaymentGatewaySinkConfig.TOKEN)) {
            if (value.toString() == null || value.toString().isEmpty()) {
                throw new ConfigException(name, value, "Token cannot be empty");
            }
        }
    }
}