package ai.pipestream.connector.service;

import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Quarkus test profile that isolates Kafka consumer state by using
 * unique topic + group id per test run.
 */
public class AccountEventsKafkaTestProfile implements QuarkusTestProfile {

    private static final String RUN_ID = UUID.randomUUID().toString();

    static String topic() {
        return "account-events-it-" + RUN_ID;
    }

    static String groupId() {
        return "connector-admin-it-" + RUN_ID;
    }

    @Override
    public Map<String, String> getConfigOverrides() {
        Map<String, String> cfg = new HashMap<>();

        // Ensure the consumer reads from a fresh topic/group each run.
        cfg.put("mp.messaging.incoming.account-events.topic", topic());
        cfg.put("mp.messaging.incoming.account-events.group.id", groupId());

        // Keep offsets deterministic for newly created topics.
        cfg.put("mp.messaging.incoming.account-events.auto.offset.reset", "earliest");

        return cfg;
    }
}
