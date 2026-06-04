package com.pinax.kafka.paymentgateway.connect.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

/** Unit tests for the shared {@link Endpoint} host:port parser. */
class EndpointTest {

    @Test
    void parsesHostPort() {
        Endpoint ep = Endpoint.parse("abp.thegraph.market:443");
        assertEquals("abp.thegraph.market", ep.host);
        assertEquals(443, ep.port);
    }

    @Test
    void parsesBracketedIpv6_stripsBrackets() {
        Endpoint ep = Endpoint.parse("[::1]:443");
        assertEquals("::1", ep.host);
        assertEquals(443, ep.port);
    }

    @Test
    void rejectsMissingPort() {
        assertThrows(ConfigException.class, () -> Endpoint.parse("gateway.test"));
        assertThrows(ConfigException.class, () -> Endpoint.parse("gateway.test:"));
    }

    @Test
    void rejectsMissingHost() {
        assertThrows(ConfigException.class, () -> Endpoint.parse(":443"));
    }

    @Test
    void rejectsNonNumericOrOutOfRangePort() {
        assertThrows(ConfigException.class, () -> Endpoint.parse("gateway.test:https"));
        assertThrows(ConfigException.class, () -> Endpoint.parse("gateway.test:0"));
        assertThrows(ConfigException.class, () -> Endpoint.parse("gateway.test:70000"));
    }

    @Test
    void rejectsSchemePathQueryFragment() {
        assertThrows(ConfigException.class, () -> Endpoint.parse("https://gateway.test:443"));
        assertThrows(ConfigException.class, () -> Endpoint.parse("gateway.test:443/report"));
        assertThrows(ConfigException.class, () -> Endpoint.parse("gateway.test:443?debug=1"));
        assertThrows(ConfigException.class, () -> Endpoint.parse("gateway.test:443#frag"));
    }

    @Test
    void rejectsWhitespace() {
        assertThrows(ConfigException.class, () -> Endpoint.parse("gateway.test :443"));
        assertThrows(ConfigException.class, () -> Endpoint.parse(" gateway.test:443"));
    }

    @Test
    void rejectsMultiColonOrUnbracketedIpv6() {
        // A scheme-like "https:host:443" and a bare (unbracketed) IPv6 are both
        // ambiguous and must be rejected; IPv6 must use the [host]:port form.
        assertThrows(ConfigException.class, () -> Endpoint.parse("https:gateway.test:443"));
        assertThrows(ConfigException.class, () -> Endpoint.parse("::1:443"));
    }

    @Test
    void rejectsMalformedBrackets() {
        assertThrows(ConfigException.class, () -> Endpoint.parse("[::1]"));      // no port
        assertThrows(ConfigException.class, () -> Endpoint.parse("[::1]443"));   // missing ':'
        assertThrows(ConfigException.class, () -> Endpoint.parse("[]:443"));     // empty host
    }

    @Test
    void rejectsNullOrEmpty() {
        assertThrows(ConfigException.class, () -> Endpoint.parse(null));
        assertThrows(ConfigException.class, () -> Endpoint.parse(""));
    }
}
