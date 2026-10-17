package io.quarkiverse.reactive.messaging.nats.jetstream.connector.configuration;

import java.time.Duration;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;

import org.eclipse.microprofile.config.Config;
import org.jspecify.annotations.NonNull;

import io.quarkiverse.reactive.messaging.nats.jetstream.connector.JetStreamConnector;
import io.quarkiverse.reactive.messaging.nats.jetstream.connector.JetStreamConnectorOutgoingConfiguration;
import io.quarkiverse.reactive.messaging.nats.jetstream.connector.reply.CorrelationIdHandler;
import io.quarkiverse.reactive.messaging.nats.jetstream.connector.reply.ReplyFailureHandler;
import io.quarkiverse.reactive.messaging.nats.jetstream.connector.reply.UuidCorrelationIdHandler;
import io.smallrye.reactive.messaging.providers.helpers.CDIUtils;
import io.smallrye.reactive.messaging.providers.impl.Configs;

@ApplicationScoped
public class PublisherChannelConfigurationFactoryImpl implements PublisherChannelConfigurationFactory {
    private final Instance<CorrelationIdHandler> correlationIdHandlers;
    private final Instance<ReplyFailureHandler> failureHandlers;

    public PublisherChannelConfigurationFactoryImpl(@Any Instance<CorrelationIdHandler> correlationIdHandlers,
            @Any Instance<ReplyFailureHandler> failureHandlers) {
        this.correlationIdHandlers = correlationIdHandlers;
        this.failureHandlers = failureHandlers;
    }

    @Override
    public @NonNull PublisherChannelConfiguration create(@NonNull Config config) {
        final var channelConfig = new JetStreamConnectorOutgoingConfiguration(config);
        return create(channelConfig.getChannel(), channelConfig);
    }

    @Override
    public @NonNull PublisherChannelConfiguration create(@NonNull String channel, @NonNull Config config) {
        final var channelConfig = new JetStreamConnectorOutgoingConfiguration(
                Configs.outgoing(config, JetStreamConnector.CONNECTOR_NAME, channel));
        return create(channel, channelConfig);
    }

    private PublisherChannelConfiguration create(@NonNull String name,
            @NonNull JetStreamConnectorOutgoingConfiguration channelConfig) {
        final var correlationIdHandlerId = nonBlank(channelConfig.getReplyCorrelationIdHandler())
                .orElse(UuidCorrelationIdHandler.ID);
        final var failureHandlerId = nonBlank(channelConfig.getReplyFailureHandler());
        return PublisherChannelConfigurationImpl.builder()
                .name(name)
                .stream(channelConfig.getStream()
                        .orElseThrow(() -> new IllegalArgumentException(String.format("Missing stream for channel: %s", name))))
                .retryBackoff(channelConfig.getRetryBackoff().map(Duration::ofMillis))
                .datasource(channelConfig.getDatasource())
                .subject(channelConfig.getSubject().orElseThrow(
                        () -> new IllegalArgumentException(String.format("Missing subject for channel: %s", name))))
                .replySubject(nonBlank(channelConfig.getReplySubject()))
                .replyTimeout(Duration.ofMillis(channelConfig.getReplyTimeout()))
                .replyInactiveThreshold(Duration.ofMillis(channelConfig.getReplyInactiveThreshold()))
                .replyCorrelationIdHandler(
                        resolveHandler(correlationIdHandlers, CorrelationIdHandler.class, name,
                                "reply.correlation-id.handler", correlationIdHandlerId))
                .replyFailureHandler(failureHandlerId.map(
                        id -> resolveHandler(failureHandlers, ReplyFailureHandler.class, name,
                                "reply.failure.handler", id)))
                .build();
    }

    private <T> T resolveHandler(final Instance<T> candidates, final Class<T> type, final String channelName,
            final String attribute, final String id) {
        try {
            return CDIUtils.getInstanceById(candidates, id).get();
        } catch (RuntimeException e) {
            throw new IllegalArgumentException(
                    "No " + type.getSimpleName() + " with id '" + id + "' found for channel '" + channelName
                            + "'. Define a CDI bean annotated with @Identifier(\"" + id + "\") or set '" + attribute
                            + "' to an available handler id.",
                    e);
        }
    }

    private Optional<String> nonBlank(final Optional<String> value) {
        return value.map(String::trim).filter(s -> !s.isEmpty());
    }
}
