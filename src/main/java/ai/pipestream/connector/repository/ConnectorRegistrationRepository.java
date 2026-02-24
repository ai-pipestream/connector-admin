package ai.pipestream.connector.repository;

import ai.pipestream.connector.entity.Connector;
import ai.pipestream.connector.entity.ConnectorConfigSchema;
import ai.pipestream.connector.entity.DataSource;
import io.quarkus.hibernate.reactive.panache.Panache;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

/**
 * Repository for connector registration operations with fully reactive database access.
 * <ul>
 *   <li>CRUD for {@link ConnectorConfigSchema}</li>
 *   <li>Updating connector type defaults (Tier 1 defaults + UI metadata)</li>
 *   <li>Assigning a connector's referenced custom config schema</li>
 * </ul>
 * <p>
 * <strong>Reactive Implementation:</strong>
 * All methods return {@link Uni} for fully non-blocking database operations using
 * Hibernate Reactive Panache. Transactions are handled via {@link Panache#withTransaction(java.util.function.Supplier)}.
 * This ensures the entire stack is reactive from gRPC service → repository → database.
 */
@ApplicationScoped
public class ConnectorRegistrationRepository {

    private static final Logger LOG = Logger.getLogger(ConnectorRegistrationRepository.class);

    // ========================================================================
    // ConnectorConfigSchema CRUD
    // ========================================================================

    /**
     * Finds a schema by its ID (reactive).
     */
    public Uni<ConnectorConfigSchema> findSchemaById(String schemaId) {
        return Panache.withSession(() -> ConnectorConfigSchema.<ConnectorConfigSchema>findById(schemaId));
    }

    /**
     * Finds a schema by connector ID and version (reactive).
     */
    public Uni<ConnectorConfigSchema> findSchemaByConnectorAndVersion(String connectorId, String schemaVersion) {
        return Panache.withSession(() -> 
            ConnectorConfigSchema.<ConnectorConfigSchema>find("connectorId = ?1 AND schemaVersion = ?2", connectorId, schemaVersion)
                .firstResult()
        );
    }

    /**
     * Lists schemas for a connector with pagination (reactive).
     */
    public Uni<List<ConnectorConfigSchema>> listSchemas(String connectorId, int limit, int offset) {
        return Panache.withSession(() -> {
            var query = ConnectorConfigSchema.<ConnectorConfigSchema>find("connectorId = ?1 ORDER BY createdAt DESC", connectorId);
            
            if (offset > 0) {
                query = query.page(offset / Math.max(limit, 1), limit);
            } else if (limit > 0) {
                query = query.page(0, limit);
            }
            
            return query.list();
        });
    }

    /**
     * Counts schemas for a connector (reactive).
     */
    public Uni<Long> countSchemas(String connectorId) {
        return Panache.withSession(() -> ConnectorConfigSchema.count("connectorId", connectorId));
    }

    /**
     * Creates a new schema (reactive).
     */
    public Uni<ConnectorConfigSchema> createSchema(String schemaId,
                                                   String connectorId,
                                                   String schemaVersion,
                                                   String customConfigSchemaJson,
                                                   String nodeCustomConfigSchemaJson,
                                                   String createdBy) {
        return Panache.withTransaction(() -> {
            String id = (schemaId != null && !schemaId.isBlank()) ? schemaId : UUID.randomUUID().toString();

            ConnectorConfigSchema schema = new ConnectorConfigSchema();
            schema.schemaId = id;
            schema.connectorId = connectorId;
            schema.schemaVersion = schemaVersion;
            schema.customConfigSchema = customConfigSchemaJson;
            schema.nodeCustomConfigSchema = nodeCustomConfigSchemaJson;
            schema.createdBy = createdBy;
            schema.syncStatus = "PENDING";
            schema.createdAt = OffsetDateTime.now();

            LOG.infof("Creating connector config schema %s for connector %s version %s", id, connectorId, schemaVersion);
            return schema.<ConnectorConfigSchema>persist()
                .invoke(persisted -> LOG.infof("Created connector config schema %s for connector %s version %s", 
                    persisted.schemaId, persisted.connectorId, persisted.schemaVersion));
        });
    }

    /**
     * Deletes a schema. Caller is responsible for ensuring it is not referenced (reactive).
     *
     * @return Uni that completes with true if deleted, false if not found
     */
    public Uni<Boolean> deleteSchema(String schemaId) {
        return Panache.withTransaction(() ->
            findSchemaById(schemaId)
                .flatMap(schema -> {
                    if (schema == null) {
                        return Uni.createFrom().item(false);
                    }
                    return schema.delete()
                        .map(v -> true);
                })
        );
    }

    /**
     * Checks if a schema is referenced by any connector (reactive).
     */
    public Uni<Boolean> isSchemaReferencedByConnector(String schemaId) {
        return Panache.withSession(() -> 
            Connector.count("customConfigSchemaId", schemaId)
                .map(count -> count > 0)
        );
    }

    /**
     * Checks if a schema is referenced by any datasource (reactive).
     */
    public Uni<Boolean> isSchemaReferencedByAnyDataSource(String schemaId) {
        return Panache.withSession(() -> 
            DataSource.count("customConfigSchemaId", schemaId)
                .map(count -> count > 0)
        );
    }

    // ========================================================================
    // Connector updates
    // ========================================================================

    /**
     * Finds a connector by ID (reactive).
     */
    public Uni<Connector> findConnectorById(String connectorId) {
        return Panache.withSession(() -> Connector.<Connector>findById(connectorId));
    }

    /**
     * Deterministic connector id: UUID.nameUUIDFromBytes(connectorType.getBytes(UTF_8)).
     * Mirrors the datasource id approach, but based solely on connectorType.
     * <p>
     * This is a pure function (no database access) so it's synchronous.
     */
    public String generateConnectorId(String connectorType) {
        return UUID.nameUUIDFromBytes(connectorType.getBytes(StandardCharsets.UTF_8)).toString();
    }

    /**
     * Sets or clears a connector's custom config schema reference (reactive).
     */
    public Uni<Connector> setConnectorCustomConfigSchema(String connectorId, String schemaIdOrEmpty) {
        return Panache.withTransaction(() ->
            findConnectorById(connectorId)
                .flatMap(connector -> {
                    if (connector == null) {
                        return Uni.createFrom().nullItem();
                    }
                    connector.customConfigSchemaId = (schemaIdOrEmpty == null || schemaIdOrEmpty.isBlank()) ? null : schemaIdOrEmpty;
                    connector.updatedAt = OffsetDateTime.now();
                    return connector.<Connector>persist();
                })
        );
    }

    /**
     * Updates connector type defaults (partial update - only non-null fields) (reactive).
     */
    public Uni<Connector> updateConnectorDefaults(String connectorId,
                                                  Boolean defaultPersistPipedocOrNull,
                                                  Integer defaultMaxInlineSizeBytesOrNull,
                                                  String defaultCustomConfigJsonOrNull,
                                                  String displayNameOrNull,
                                                  String ownerOrNull,
                                                  String documentationUrlOrNull,
                                                  List<String> tagsOrNull) {
        return Panache.withTransaction(() ->
            findConnectorById(connectorId)
                .flatMap(connector -> {
                    if (connector == null) {
                        return Uni.createFrom().nullItem();
                    }

                    boolean changed = false;

                    if (defaultPersistPipedocOrNull != null) {
                        connector.defaultPersistPipedoc = defaultPersistPipedocOrNull;
                        changed = true;
                    }
                    if (defaultMaxInlineSizeBytesOrNull != null) {
                        connector.defaultMaxInlineSizeBytes = defaultMaxInlineSizeBytesOrNull;
                        changed = true;
                    }
                    if (defaultCustomConfigJsonOrNull != null) {
                        connector.defaultCustomConfig = defaultCustomConfigJsonOrNull;
                        changed = true;
                    }
                    if (displayNameOrNull != null && !displayNameOrNull.isBlank()) {
                        connector.displayName = displayNameOrNull;
                        changed = true;
                    }
                    if (ownerOrNull != null && !ownerOrNull.isBlank()) {
                        connector.owner = ownerOrNull;
                        changed = true;
                    }
                    if (documentationUrlOrNull != null && !documentationUrlOrNull.isBlank()) {
                        connector.documentationUrl = documentationUrlOrNull;
                        changed = true;
                    }
                    if (tagsOrNull != null) {
                        connector.tags = tagsOrNull;
                        changed = true;
                    }

                    if (changed) {
                        connector.updatedAt = OffsetDateTime.now();
                        return connector.<Connector>persist();
                    } else {
                        return Uni.createFrom().item(connector);
                    }
                })
        );
    }
}
