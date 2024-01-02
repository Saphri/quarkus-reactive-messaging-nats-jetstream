package io.quarkiverse.reactive.messsaging.nats.jetstream.test;

import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.logging.Logger;

import io.quarkiverse.reactive.messsaging.nats.jetstream.JetStreamIncomingMessageMetadata;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.annotations.Blocking;

@ApplicationScoped
public class DataConsumingBean {
    private final static Logger logger = Logger.getLogger(DataConsumingBean.class);

    volatile Optional<Data> lastData = Optional.empty();

    @Blocking
    @Incoming("data-consumer")
    public Uni<Void> data(Message<String> message) {
        return Uni.createFrom().item(message)
                .onItem().invoke(this::handleData)
                .onItem().ignore().andContinueWithNull();
    }

    public Optional<Data> getLast() {
        return lastData;
    }

    private void handleData(Message<String> message) {
        logger.infof("Received message: %s", message);
        message.getMetadata(JetStreamIncomingMessageMetadata.class)
                .ifPresent(metadata -> lastData = Optional.of(
                        new Data(message.getPayload(), metadata.headers().get("RESOURCE_ID").get(0), metadata.messageId())));
        message.ack();
    }
}
