package ai.pipestream.connector.service;

import ai.pipestream.connector.entity.Connector;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.hibernate.reactive.panache.Panache;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.vertx.core.runtime.context.VertxContextSafetyToggle;
import io.smallrye.common.vertx.VertxContext;
import io.smallrye.mutiny.Uni;
import io.vertx.core.Context;
import io.vertx.mutiny.core.Vertx;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

/**
 * Loads connector type seed data from connectors-seed.json at startup.
 * <p>
 * Upserts each entry — existing connector types are left untouched
 * (ON CONFLICT DO NOTHING semantics via find-before-persist).
 * The JSON seed is additive to the Flyway V1 SQL seed.
 */
@ApplicationScoped
public class ConnectorTypeSeedLoader {

    private static final Logger LOG = Logger.getLogger(ConnectorTypeSeedLoader.class);
    private static final String SEED_RESOURCE = "connectors-seed.json";

    @Inject
    Vertx vertx;

    /**
     * Observes Quarkus startup and seeds connector types from the JSON resource.
     * <p>
     * Panache requires a duplicated Vertx context marked as safe for reactive
     * session operations. The startup event fires on the main thread (no Vertx
     * context), so we use {@code VertxContext.getOrCreateDuplicatedContext(vertx)}
     * to obtain one from the Vertx instance, then set the safety flag.
     * This follows the same pattern as Quarkus's own {@code VertxContextSupport}.
     * <p>
     * Failures are logged but do not prevent application startup.
     */
    void onStartup(@Observes StartupEvent event) {
        List<SeedEntry> entries = loadSeedFile();
        if (entries == null || entries.isEmpty()) {
            LOG.info("No connector seed entries found, skipping");
            return;
        }

        LOG.infof("Seeding %d connector types from %s", entries.size(), SEED_RESOURCE);

        try {
            // Create a safe duplicated context from the Vertx instance.
            // On the main thread there's no Vertx context, so we need to go through the instance.
            Context safeContext = VertxContext.getOrCreateDuplicatedContext(vertx.getDelegate());
            VertxContextSafetyToggle.setContextSafe(safeContext, true);

            // Chain upserts sequentially on the safe context for Panache session access
            Uni<Void> chain = Uni.createFrom().voidItem()
                .emitOn(runnable -> safeContext.runOnContext(v -> runnable.run()));

            for (SeedEntry entry : entries) {
                chain = chain.flatMap(v -> upsertConnectorType(entry));
            }

            chain.await().indefinitely();
            LOG.infof("Connector type seeding complete");
        } catch (Exception e) {
            // Don't block startup — Flyway V1 already seeds the base connectors
            LOG.warnf(e, "Connector type seeding failed (non-fatal, Flyway seed is primary)");
        }
    }

    /**
     * Upserts a single connector type: creates if missing, skips if exists.
     * Looks up by connector_type (not connector_id) because the Flyway V1 seed
     * used hardcoded UUIDs that differ from the deterministic generation.
     */
    private Uni<Void> upsertConnectorType(SeedEntry entry) {
        String connectorId = UUID.nameUUIDFromBytes(
            entry.connectorType.getBytes(StandardCharsets.UTF_8)).toString();

        return Panache.withTransaction(() ->
            Connector.<Connector>find("connectorType", entry.connectorType).firstResult()
                .flatMap(existing -> {
                    if (existing != null) {
                        LOG.debugf("Connector type '%s' already exists (id=%s), skipping",
                            entry.connectorType, existing.connectorId);
                        return Uni.createFrom().voidItem();
                    }
                    Connector connector = new Connector(
                        connectorId,
                        entry.connectorType,
                        entry.name,
                        entry.description,
                        entry.managementType != null ? entry.managementType : "UNMANAGED"
                    );
                    LOG.infof("Seeding connector type '%s' with id %s", entry.connectorType, connectorId);
                    return connector.persist().replaceWithVoid();
                })
        );
    }

    /**
     * Reads and parses the seed JSON from the classpath.
     */
    private List<SeedEntry> loadSeedFile() {
        try (InputStream is = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream(SEED_RESOURCE)) {
            if (is == null) {
                LOG.warnf("Seed file %s not found on classpath", SEED_RESOURCE);
                return List.of();
            }
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(is, new TypeReference<>() {});
        } catch (Exception e) {
            LOG.errorf(e, "Failed to load connector seed file %s", SEED_RESOURCE);
            return List.of();
        }
    }

    /**
     * JSON mapping for seed file entries.
     */
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
