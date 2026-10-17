package io.quarkiverse.reactive.messaging.nats.jetstream.connector.processors;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;

import org.eclipse.microprofile.config.Config;
import org.jspecify.annotations.NonNull;

import io.quarkiverse.reactive.messaging.nats.jetstream.client.Client;
import io.quarkiverse.reactive.messaging.nats.jetstream.connector.configuration.ClientResolver;
import io.quarkiverse.reactive.messaging.nats.jetstream.connector.configuration.PublisherChannelConfigurationFactory;

@ApplicationScoped
public class MessageSubscriberProcessorFactory {
    private final ClientResolver clientResolver;
    private final PublisherChannelConfigurationFactory publisherChannelConfigurationFactory;

    public MessageSubscriberProcessorFactory(@Any Instance<Client> clients,
            PublisherChannelConfigurationFactory publisherChannelConfigurationFactory) {
        this.clientResolver = new ClientResolver(clients);
        this.publisherChannelConfigurationFactory = publisherChannelConfigurationFactory;
    }

    public <T> MessageSubscriberProcessor<T> create(@NonNull final Config configuration) {
        final var channelConfiguration = publisherChannelConfigurationFactory.create(configuration);
        return new MessageSubscriberProcessor<>(
                channelConfiguration,
                clientResolver.client(channelConfiguration.datasource()));
    }

}
