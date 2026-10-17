# Glossary

Terms used in the runtime code and its documentation.

**Datasource**
: A named NATS connection and the JetStream resources (streams, consumers, key-values, object stores) declared for it under `quarkus.messaging.nats.data-sources.<name>`. Each datasource gets its own `Client` bean, identified by the datasource name. Channels and resources that do not name a datasource use the `default` datasource (`ConnectorConfiguration.DEFAULT_DATASOURCE`).

**Channel configuration factory**
: `PublisherChannelConfigurationFactory` (outgoing channels) and `ConsumerChannelConfigurationFactory` (incoming channels). They turn a channel's MicroProfile `Config` into a typed `PublisherChannelConfiguration` or `ConsumerChannelConfiguration`. Both go through the classes generated from the `@ConnectorAttribute` annotations on `JetStreamConnector`, so those annotations are the only place channel attribute names and defaults are declared.

**Client resolver**
: `ClientResolver`, the single place that maps a datasource name, or its absence, to the `Client` bean for that datasource.
