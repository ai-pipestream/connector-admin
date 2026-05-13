package ai.pipestream.connector.service;

import ai.pipestream.connector.entity.Connector;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.transaction.Transactional;
import org.jboss.logging.Logger;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

/**
 * Loads connector type seed data from connectors-seed.json at startup.
 */
@ApplicationScoped
public class ConnectorTypeSeedLoader {

    private static final Logger LOG = Logger.getLogger(ConnectorTypeSeedLoader.class);
    private static final String SEED_RESOURCE = "connectors-seed.json";

    void onStartup(@Observes StartupEvent event) {
        List<SeedEntry> entries = loadSeedFile();
        if (entries == null || entries.isEmpty()) {
            LOG.info("No connector seed entries found, skipping");
            return;
        }

        try {
            LOG.infof("Seeding %d connector types from %s", entries.size(), SEED_RESOURCE);
            entries.forEach(this::upsertConnectorType);
            LOG.info("Connector type seeding complete");
        } catch (Exception e) {
            LOG.warnf(e, "Connector type seeding failed (non-fatal, Flyway seed is primary)");
        }
    }

    @Transactional
    void upsertConnectorType(SeedEntry entry) {
        Connector existing = Connector.find("connectorType", entry.connectorType).firstResult();
        if (existing != null) {
            LOG.debugf("Connector type '%s' already exists (id=%s), skipping", entry.connectorType, existing.connectorId);
            return;
        }

        String connectorId = UUID.nameUUIDFromBytes(entry.connectorType.getBytes(StandardCharsets.UTF_8)).toString();
        Connector connector = new Connector(
            connectorId,
            entry.connectorType,
            entry.name,
            entry.description,
            entry.managementType != null ? entry.managementType : "UNMANAGED"
        );
        LOG.infof("Seeding connector type '%s' with id %s", entry.connectorType, connectorId);
        connector.persist();
    }

    private List<SeedEntry> loadSeedFile() {
        try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(SEED_RESOURCE)) {
            if (is == null) {
                LOG.warnf("Seed file %s not found on classpath", SEED_RESOURCE);
                return List.of();
            }
            return new ObjectMapper().readValue(is, new TypeReference<>() {});
        } catch (Exception e) {
            LOG.errorf(e, "Failed to load connector seed file %s", SEED_RESOURCE);
            return List.of();
        }
    }

    static class SeedEntry {
        @JsonProperty("connector_type")
        public String connectorType;
        @JsonProperty("name")
        public String name;
        @JsonProperty("description")
        public String description;
        @JsonProperty("management_type")
        public String managementType;
    }
}
