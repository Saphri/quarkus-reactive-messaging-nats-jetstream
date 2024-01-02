package io.quarkiverse.reactive.messsaging.nats.jetstream.client;

import static io.nats.client.Connection.Status.CONNECTED;

import java.io.IOException;

import org.jboss.logging.Logger;

import io.nats.client.Dispatcher;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.vertx.mutiny.core.Context;

public class Connection implements AutoCloseable {
    private final static Logger logger = Logger.getLogger(Connection.class);

    private final io.nats.client.Connection connection;
    private final Context context;

    public Connection(final io.nats.client.Connection connection, final Context context) {
        this.connection = connection;
        this.context = context;
    }

    public io.nats.client.Connection connection() {
        return connection;
    }

    public Context context() {
        return context;
    }

    public JetStream jetStream() throws IOException {
        return connection.jetStream();
    }

    public JetStreamManagement jetStreamManagement() throws IOException {
        return connection.jetStreamManagement();
    }

    public Dispatcher createDispatcher() {
        return connection.createDispatcher();
    }

    public io.nats.client.Connection.Status getStatus() {
        return connection.getStatus();
    }

    public boolean isConnected() {
        return CONNECTED.equals(getStatus());
    }

    @Override
    public void close() {
        try {
            connection.close();
        } catch (InterruptedException e) {
            logger.warn(e.getMessage(), e);
        }
    }
}
