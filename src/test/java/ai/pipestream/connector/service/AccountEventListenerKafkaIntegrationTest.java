package ai.pipestream.connector.service;

import ai.pipestream.connector.entity.Connector;
import ai.pipestream.connector.entity.ConnectorAccount;
import ai.pipestream.connector.repository.ConnectorRepository;
import ai.pipestream.repository.v1.account.AccountEvent;
import com.google.protobuf.Timestamp;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.UUIDSerializer;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end integration test proving that:
 * - Kafka Dev Services boots a broker
 * - Apicurio Registry Dev Services (via quarkus-apicurio-registry-protobuf) boots a registry
 * - The Protobuf Kafka deserializer is wired for the incoming channel
 * - {@link AccountEventListener} consumes an {@link AccountEvent} and applies DB side-effects
 */
@QuarkusTest
@TestProfile(AccountEventsKafkaTestProfile.class)
public class AccountEventListenerKafkaIntegrationTest {

    // Reuse a single HttpClient instance (no need for try-with-resources; it's not Closeable)
    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();

    @Inject
    ConnectorRepository connectorRepository;

    @BeforeEach
    @Transactional
    void cleanup() {
        ConnectorAccount.deleteAll();
        Connector.deleteAll();
    }

    @Test
    void accountInactivatedEventDisablesLinkedConnectors() throws Exception {
        String accountId = "acct-" + UUID.randomUUID();

        // Seed DB: one active connector linked to accountId
        Connector connector = connectorRepository.createConnector(
                "kafka-it-" + System.currentTimeMillis(),
                "filesystem",
                "Kafka integration test",
                "hash"
        );
        connectorRepository.linkConnectorToAccount(connector.connectorId, accountId);

        assertTrue(connectorRepository.findByConnectorId(connector.connectorId).active);

        // Ensure registry is up (Dev Services)
        String registryUrl = ConfigProvider.getConfig()
                .getValue("mp.messaging.connector.smallrye-kafka.apicurio.registry.url", String.class);
        waitForHttpOk(healthUrlForRegistry(registryUrl), Duration.ofSeconds(45));

        // Resolve Kafka bootstrap servers from runtime config
        String bootstrapServers = ConfigProvider.getConfig().getValue("kafka.bootstrap.servers", String.class);

        // Ensure topic exists
        String topic = ConfigProvider.getConfig().getValue("mp.messaging.incoming.account-events.topic", String.class);
        createTopicIfMissing(bootstrapServers, topic);

        // Produce an AccountEvent.Inactivated (UUID key + Apicurio Protobuf value)
        UUID key = UUID.nameUUIDFromBytes(accountId.getBytes());
        AccountEvent event = AccountEvent.newBuilder()
                .setEventId("evt-" + System.currentTimeMillis())
                .setTimestamp(Timestamp.newBuilder()
                        .setSeconds(Instant.now().getEpochSecond())
                        .setNanos(Instant.now().getNano())
                        .build())
                .setAccountId(accountId)
                .setInactivated(AccountEvent.Inactivated.newBuilder()
                        .setReason("integration-test")
                        .build())
                .build();

        produceAccountEvent(bootstrapServers, registryUrl, topic, key, event);

        // Assert consumer applied side-effect
        waitUntil(Duration.ofSeconds(30), () -> {
            Connector updated = connectorRepository.findByConnectorId(connector.connectorId);
            return updated != null && !updated.active && "account_inactive".equals(updated.statusReason);
        });

        Connector updated = connectorRepository.findByConnectorId(connector.connectorId);
        assertNotNull(updated);
        assertFalse(updated.active);
        assertEquals("account_inactive", updated.statusReason);
    }

    private static void produceAccountEvent(
            String bootstrapServers,
            String registryUrl,
            String topic,
            UUID key,
            AccountEvent event
    ) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, UUIDSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "io.apicurio.registry.serde.protobuf.ProtobufKafkaSerializer");

        // Apicurio Registry serde configuration (producer-side)
        props.put("apicurio.registry.url", registryUrl);
        props.put("apicurio.registry.auto-register", "true");
        props.put("apicurio.registry.find-latest", "true");
        props.put("apicurio.registry.artifact-resolver-strategy",
                "io.apicurio.registry.serde.strategy.SimpleTopicIdStrategy");
        props.put("apicurio.protobuf.derive.class", "true");

        try (KafkaProducer<UUID, AccountEvent> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>(topic, key, event)).get(30, TimeUnit.SECONDS);
            producer.flush();
        }
    }

    private static void createTopicIfMissing(String bootstrapServers, String topic) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        try (AdminClient admin = AdminClient.create(props)) {
            try {
                admin.createTopics(java.util.List.of(new NewTopic(topic, 1, (short) 1)))
                        .all().get(15, TimeUnit.SECONDS);
            } catch (Exception e) {
                // If topic already exists, ignore.
                if (e.getCause() instanceof TopicExistsException) {
                    return;
                }
                throw e;
            }
        }
    }

    private static URI healthUrlForRegistry(String registryUrl) {
        // DevServices config provides .../apis/registry/v3; health is at /health
        URI uri = URI.create(registryUrl);
        String base = uri.getScheme() + "://" + uri.getHost() + (uri.getPort() > 0 ? (":" + uri.getPort()) : "");
        return URI.create(base + "/health");
    }

    private static void waitForHttpOk(URI url, Duration timeout) {
        await()
            .atMost(timeout)
            .pollInterval(Duration.ofMillis(250))
            .ignoreExceptions()
            .untilAsserted(() -> {
                HttpRequest req = HttpRequest.newBuilder(url)
                        .timeout(Duration.ofSeconds(5))
                        .GET()
                        .build();
                int code = HTTP_CLIENT
                        .send(req, HttpResponse.BodyHandlers.discarding())
                        .statusCode();
                assertTrue(code >= 200 && code < 300, "Expected 2xx from " + url + " but got " + code);
            });
    }

    private static void waitUntil(Duration timeout, java.util.concurrent.Callable<Boolean> condition) {
        await()
            .atMost(timeout)
            .pollInterval(Duration.ofMillis(200))
            .ignoreExceptions()
            .until(() -> {
                try {
                    return Boolean.TRUE.equals(condition.call());
                } catch (Exception e) {
                    // Treat exceptions as a signal to keep waiting
                    return false;
                }
            });
    }
}
