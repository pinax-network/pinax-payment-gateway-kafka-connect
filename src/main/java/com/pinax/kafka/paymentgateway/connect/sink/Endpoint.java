package com.pinax.kafka.paymentgateway.connect.sink;

import org.apache.kafka.common.config.ConfigException;

/**
 * A parsed gRPC {@code host:port} endpoint. This is the single source of truth for
 * endpoint parsing — both {@link PaymentGatewaySinkConfigValidator} (at config time)
 * and {@link PaymentGatewayClient} (when opening the channel) go through
 * {@link #parse(String)}, so validation and the actual host/port used can never drift.
 *
 * <p>IPv6 literals must be bracketed ({@code [::1]:443}); the brackets are stripped
 * from {@link #host} so it can be handed straight to the gRPC channel builder.
 */
final class Endpoint {
    final String host;
    final int port;

    private Endpoint(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /**
     * Parses {@code host:port} (or {@code [ipv6]:port}). This is a gRPC endpoint, not a
     * URL — a scheme, path, query or fragment is rejected.
     *
     * @throws ConfigException if the value is not a valid {@code host:port}
     */
    static Endpoint parse(String endpoint) {
        if (endpoint == null || endpoint.isEmpty()) {
            throw new ConfigException("Endpoint must be of the form host:port");
        }
        if (endpoint.chars().anyMatch(Character::isWhitespace)) {
            throw new ConfigException("Endpoint must not contain whitespace: " + endpoint);
        }
        // Reject a scheme, path, query or fragment up front so e.g. "https://h:443",
        // "h:443/p" or "h:443?x=1" get a clear message instead of failing later as a
        // non-numeric port.
        if (endpoint.indexOf('/') >= 0 || endpoint.indexOf('?') >= 0 || endpoint.indexOf('#') >= 0) {
            throw new ConfigException(
                    "Endpoint must be host:port, without a scheme, path, query or fragment: " + endpoint);
        }

        String host;
        String portStr;
        if (endpoint.charAt(0) == '[') {
            // Bracketed IPv6: [host]:port
            int close = endpoint.indexOf(']');
            if (close <= 1 || close + 1 >= endpoint.length() || endpoint.charAt(close + 1) != ':') {
                throw new ConfigException("Bracketed IPv6 endpoint must be of the form [host]:port: " + endpoint);
            }
            host = endpoint.substring(1, close);
            portStr = endpoint.substring(close + 2);
        } else {
            // Exactly one ':' for a plain host:port. More than one (e.g.
            // "https:host:443" or a bare IPv6 like "::1:443") is ambiguous — IPv6
            // hosts must be bracketed.
            int sep = endpoint.indexOf(':');
            if (sep != endpoint.lastIndexOf(':')) {
                throw new ConfigException(
                        "Endpoint must be host:port with a single ':'; bracket IPv6 hosts as [host]:port: " + endpoint);
            }
            if (sep <= 0 || sep == endpoint.length() - 1) {
                throw new ConfigException("Endpoint must be of the form host:port: " + endpoint);
            }
            host = endpoint.substring(0, sep);
            portStr = endpoint.substring(sep + 1);
        }

        final int port;
        try {
            port = Integer.parseInt(portStr);
        } catch (NumberFormatException e) {
            throw new ConfigException("Endpoint port must be a number: " + endpoint);
        }
        if (port < 1 || port > 65535) {
            throw new ConfigException("Endpoint port must be between 1 and 65535: " + endpoint);
        }

        return new Endpoint(host, port);
    }
}
