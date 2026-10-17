package io.quarkiverse.reactive.messaging.nats.jetstream.connector.configuration;

import static io.quarkiverse.reactive.messaging.nats.jetstream.connector.configuration.ConnectorConfiguration.DEFAULT_DATASOURCE;

import java.util.Optional;

import jakarta.enterprise.inject.Instance;

import org.jspecify.annotations.NonNull;

import io.quarkiverse.reactive.messaging.nats.jetstream.client.Client;
import io.smallrye.reactive.messaging.providers.helpers.CDIUtils;

/**
 * Resolves the {@link Client} bean for a datasource. Channels and resources that do not name a datasource use
 * {@link ConnectorConfiguration#DEFAULT_DATASOURCE}.
 */
public class ClientResolver {
    private final Instance<Client> clients;

    public ClientResolver(@NonNull Instance<Client> clients) {
        this.clients = clients;
    }

    /**
     * Returns the client registered for the given datasource, or for the default datasource when none is given.
     *
     * @param datasource the datasource name, empty for the default datasource
     * @return the {@link Client} bean identified by the datasource name
     */
    public @NonNull Client client(@NonNull Optional<String> datasource) {
        return client(datasource.orElse(DEFAULT_DATASOURCE));
    }

    /**
     * Returns the client registered for the given datasource.
     *
     * @param datasource the datasource name
     * @return the {@link Client} bean identified by the datasource name
     */
    public @NonNull Client client(@NonNull String datasource) {
        return CDIUtils.getInstanceById(clients, datasource).get();
    }
}
