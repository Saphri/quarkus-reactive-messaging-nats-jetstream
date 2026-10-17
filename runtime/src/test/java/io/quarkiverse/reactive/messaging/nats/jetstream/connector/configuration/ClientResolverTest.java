package io.quarkiverse.reactive.messaging.nats.jetstream.connector.configuration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.quarkiverse.reactive.messaging.nats.jetstream.client.Client;

class ClientResolverTest {
    private final Client defaultClient = client("default");
    private final Client otherClient = client("other");
    private final ClientResolver resolver = new ClientResolver(
            StubInstances.byIdentifier(Map.of(ConnectorConfiguration.DEFAULT_DATASOURCE, defaultClient, "other", otherClient)));

    @Test
    void resolvesDefaultDatasourceWhenNoneGiven() {
        assertThat(resolver.client(Optional.empty())).isSameAs(defaultClient);
    }

    @Test
    void resolvesNamedDatasource() {
        assertThat(resolver.client(Optional.of("other"))).isSameAs(otherClient);
        assertThat(resolver.client("other")).isSameAs(otherClient);
    }

    @Test
    void failsForUnknownDatasource() {
        assertThatThrownBy(() -> resolver.client(Optional.of("missing"))).isInstanceOf(RuntimeException.class);
    }

    private static Client client(final String name) {
        return (Client) Proxy.newProxyInstance(Client.class.getClassLoader(), new Class<?>[] { Client.class },
                (proxy, method, args) -> {
                    if (method.getName().equals("toString")) {
                        return "client-" + name;
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
    }
}
