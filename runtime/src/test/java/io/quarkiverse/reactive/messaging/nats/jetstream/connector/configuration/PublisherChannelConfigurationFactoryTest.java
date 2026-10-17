package io.quarkiverse.reactive.messaging.nats.jetstream.connector.configuration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.eclipse.microprofile.config.Config;
import org.junit.jupiter.api.Test;

import io.quarkiverse.reactive.messaging.nats.jetstream.connector.JetStreamConnector;
import io.quarkiverse.reactive.messaging.nats.jetstream.connector.reply.CorrelationIdHandler;
import io.quarkiverse.reactive.messaging.nats.jetstream.connector.reply.ReplyFailureHandler;
import io.quarkiverse.reactive.messaging.nats.jetstream.connector.reply.UuidCorrelationIdHandler;
import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfigBuilder;
import io.smallrye.reactive.messaging.providers.impl.Configs;

class PublisherChannelConfigurationFactoryTest {
    private static final String CHANNEL = "requests";
    private static final String PREFIX = "mp.messaging.outgoing." + CHANNEL + ".";

    private final CorrelationIdHandler uuidHandler = new UuidCorrelationIdHandler();
    private final ReplyFailureHandler failureHandler = payload -> Optional.empty();
    private final PublisherChannelConfigurationFactory factory = new PublisherChannelConfigurationFactoryImpl(
            StubInstances.byIdentifier(Map.of(UuidCorrelationIdHandler.ID, uuidHandler)),
            StubInstances.byIdentifier(Map.of("failures", failureHandler)));

    @Test
    void appliesConnectorAttributeDefaultsForNamedChannel() {
        final var configuration = factory.create(CHANNEL, config(Map.of("stream", "s", "subject", "orders")));

        assertDefaults(configuration);
    }

    @Test
    void appliesConnectorAttributeDefaultsForChannelConfig() {
        final var configuration = factory.create(channelConfig(Map.of("stream", "s", "subject", "orders")));

        assertDefaults(configuration);
    }

    @Test
    void bothPathsProduceTheSameConfiguration() {
        final var properties = Map.of("stream", "s", "subject", "orders", "datasource", "other", "retry-backoff", "250",
                "reply.subject", "orders.answers", "reply.timeout", "1500", "reply.inactive-threshold", "0",
                "reply.failure.handler", "failures");

        final var named = factory.create(CHANNEL, config(properties));
        final var channel = factory.create(channelConfig(properties));

        assertThat(named).isEqualTo(channel);
        assertThat(named.datasource()).contains("other");
        assertThat(named.retryBackoff()).contains(Duration.ofMillis(250));
        assertThat(named.replySubject()).contains("orders.answers");
        assertThat(named.replyTimeout()).isEqualTo(Duration.ofMillis(1500));
        assertThat(named.replyInactiveThreshold()).isEqualTo(Duration.ZERO);
        assertThat(named.replyFailureHandler()).containsSame(failureHandler);
    }

    @Test
    void missingStreamNamesTheChannel() {
        assertThatThrownBy(() -> factory.create(CHANNEL, config(Map.of("subject", "orders"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Missing stream for channel: " + CHANNEL);
    }

    @Test
    void missingSubjectNamesTheChannel() {
        assertThatThrownBy(() -> factory.create(CHANNEL, config(Map.of("stream", "s"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Missing subject for channel: " + CHANNEL);
    }

    @Test
    void unknownHandlerIdIsReportedWithChannelAndAttribute() {
        assertThatThrownBy(() -> factory.create(CHANNEL,
                config(Map.of("stream", "s", "subject", "orders", "reply.correlation-id.handler", "nope"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("No CorrelationIdHandler with id 'nope' found for channel '" + CHANNEL
                        + "'. Define a CDI bean annotated with @Identifier(\"nope\") or set "
                        + "'reply.correlation-id.handler' to an available handler id.");
    }

    private static void assertDefaults(final PublisherChannelConfiguration configuration) {
        assertThat(configuration.name()).isEqualTo(CHANNEL);
        assertThat(configuration.stream()).isEqualTo("s");
        assertThat(configuration.subject()).isEqualTo("orders");
        assertThat(configuration.replyTimeout()).isEqualTo(Duration.ofMillis(5000));
        assertThat(configuration.replyInactiveThreshold()).isEqualTo(Duration.ofMillis(60000));
        assertThat(configuration.replySubject()).isEmpty();
        assertThat(configuration.datasource()).isEmpty();
        assertThat(configuration.retryBackoff()).isEmpty();
        assertThat(configuration.replyCorrelationIdHandler()).isInstanceOf(UuidCorrelationIdHandler.class);
        assertThat(configuration.replyFailureHandler()).isEmpty();
    }

    private static Config channelConfig(final Map<String, String> channelProperties) {
        return Configs.outgoing(config(channelProperties), JetStreamConnector.CONNECTOR_NAME, CHANNEL);
    }

    private static Config config(final Map<String, String> channelProperties) {
        final var properties = new HashMap<String, String>();
        properties.put(PREFIX + "connector", JetStreamConnector.CONNECTOR_NAME);
        channelProperties.forEach((key, value) -> properties.put(PREFIX + key, value));
        return new SmallRyeConfigBuilder()
                .withSources(new PropertiesConfigSource(properties, "test", 100))
                .build();
    }
}
