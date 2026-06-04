package com.pinax.kafka.paymentgateway.connect.sink;

import java.net.URI;
import java.net.URISyntaxException;

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
        final URI uri;
        try {
            uri = new URI((String) value);
        } catch (URISyntaxException e) {
            // Only the parse can throw here; the specific ConfigExceptions below
            // are no longer swallowed and re-wrapped with a generic message.
            throw new ConfigException(name, value, "Not a valid URL: " + e.getMessage());
        }

        String scheme = uri.getScheme();
        // URI schemes are case-insensitive (RFC 3986) and the client treats them
        // that way, so accept HTTP/HTTPS in any case.
        if (scheme == null || (!scheme.equalsIgnoreCase("http") && !scheme.equalsIgnoreCase("https"))) {
            throw new ConfigException(name, value, "Not a valid URL, scheme must be http or https");
        }
        if (uri.getHost() == null || uri.getHost().isEmpty()) {
            throw new ConfigException(name, value, "Not a valid URL, missing host");
        }
        if (uri.getPort() == -1) {
            throw new ConfigException(name, value, "Not a valid URL, missing port");
        }
        // The client only uses scheme/host/port. Reject anything else (embedded
        // credentials, a path, query or fragment) so it can't be silently ignored
        // — or, in the case of user-info, accidentally logged.
        String path = uri.getPath();
        if (uri.getUserInfo() != null
                || (path != null && !path.isEmpty() && !path.equals("/"))
                || uri.getQuery() != null
                || uri.getFragment() != null) {
            throw new ConfigException(name, value, "Endpoint must be of the form scheme://host:port");
        }
    }
}
