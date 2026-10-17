package io.quarkiverse.reactive.messaging.nats.jetstream.connector.processors;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;

import org.eclipse.microprofile.config.Config;
import org.jspecify.annotations.NonNull;

import io.quarkiverse.reactive.messaging.nats.jetstream.client.Client;
import io.quarkiverse.reactive.messaging.nats.jetstream.connector.configuration.ClientResolver;
import io.quarkiverse.reactive.messaging.nats.jetstream.connector.configuration.ConsumerChannelConfigurationFactory;

@ApplicationScoped
public class MessagePublisherProcessorFactory {
    private final ClientResolver clientResolver;
    private final ConsumerChannelConfigurationFactory channelConfigurationFactory;

    public MessagePublisherProcessorFactory(@Any Instance<Client> clients,
            ConsumerChannelConfigurationFactory channelConfigurationFactory) {
        this.clientResolver = new ClientResolver(clients);
        this.channelConfigurationFactory = channelConfigurationFactory;
    }

    public MessagePublisherProcessor<?> create(@NonNull final Config configuration) {
        final var channelConfiguration = channelConfigurationFactory.create(configuration);
        return new MessagePublisherProcessor<>(
                channelConfiguration,
                clientResolver.client(channelConfiguration.datasource()));
    }

}
