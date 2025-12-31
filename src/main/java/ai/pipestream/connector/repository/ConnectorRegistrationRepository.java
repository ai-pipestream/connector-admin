package ai.pipestream.connector.repository;

import ai.pipestream.connector.entity.Connector;
import ai.pipestream.connector.entity.ConnectorConfigSchema;
import ai.pipestream.connector.entity.DataSource;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import org.jboss.logging.Logger;

import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

/**
 * Repository for connector registration operations:
 * <ul>
 *   <li>CRUD for {@link ConnectorConfigSchema}</li>
 *   <li>Updating connector type defaults (Tier 1 defaults + UI metadata)</li>
 *   <li>Assigning a connector's referenced custom config schema</li>
 * </ul>
 */
@ApplicationScoped
public class ConnectorRegistrationRepository {

    private static final Logger LOG = Logger.getLogger(ConnectorRegistrationRepository.class);

    @Inject
    EntityManager entityManager;

    // ========================================================================
    // ConnectorConfigSchema CRUD
    // ========================================================================

    @Transactional
    public ConnectorConfigSchema findSchemaById(String schemaId) {
        return ConnectorConfigSchema.findById(schemaId);
    }

    @Transactional
    public ConnectorConfigSchema findSchemaByConnectorAndVersion(String connectorId, String schemaVersion) {
        return ConnectorConfigSchema.find("connectorId = ?1 AND schemaVersion = ?2", connectorId, schemaVersion)
            .firstResult();
    }

    @Transactional
    public List<ConnectorConfigSchema> listSchemas(String connectorId, int limit, int offset) {
        String query = "FROM ConnectorConfigSchema s WHERE s.connectorId = :connectorId ORDER BY s.createdAt DESC";
        var typedQuery = entityManager.createQuery(query, ConnectorConfigSchema.class)
            .setParameter("connectorId", connectorId);

        if (offset > 0) {
            typedQuery.setFirstResult(offset);
        }
        if (limit > 0) {
            typedQuery.setMaxResults(limit);
        }

        return typedQuery.getResultList();
    }

    @Transactional
    public long countSchemas(String connectorId) {
        return ConnectorConfigSchema.count("connectorId", connectorId);
    }

    @Transactional
    public ConnectorConfigSchema createSchema(String schemaId,
                                             String connectorId,
                                             String schemaVersion,
                                             String customConfigSchemaJson,
                                             String nodeCustomConfigSchemaJson,
                                             String createdBy) {
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
        schema.persist();

        LOG.infof("Created connector config schema %s for connector %s version %s", id, connectorId, schemaVersion);
        return schema;
    }

    /**
     * Deletes a schema. Caller is responsible for ensuring it is not referenced.
     *
     * @return true if deleted, false if not found
     */
    @Transactional
    public boolean deleteSchema(String schemaId) {
        ConnectorConfigSchema schema = findSchemaById(schemaId);
        if (schema == null) {
            return false;
        }
        schema.delete();
        return true;
    }

    @Transactional
    public boolean isSchemaReferencedByConnector(String schemaId) {
        return Connector.count("customConfigSchemaId", schemaId) > 0;
    }

    @Transactional
    public boolean isSchemaReferencedByAnyDataSource(String schemaId) {
        return DataSource.count("customConfigSchemaId", schemaId) > 0;
    }

    // ========================================================================
    // Connector updates
    // ========================================================================

    @Transactional
    public Connector findConnectorById(String connectorId) {
        return Connector.findById(connectorId);
    }

    /**
     * Deterministic connector id: UUID.nameUUIDFromBytes(connectorType.getBytes(UTF_8)).
     * Mirrors the datasource id approach, but based solely on connectorType.
     */
    public String generateConnectorId(String connectorType) {
        return UUID.nameUUIDFromBytes(connectorType.getBytes(StandardCharsets.UTF_8)).toString();
    }

    @Transactional
    public Connector setConnectorCustomConfigSchema(String connectorId, String schemaIdOrEmpty) {
        Connector connector = findConnectorById(connectorId);
        if (connector == null) {
            return null;
        }
        connector.customConfigSchemaId = (schemaIdOrEmpty == null || schemaIdOrEmpty.isBlank()) ? null : schemaIdOrEmpty;
        connector.updatedAt = OffsetDateTime.now();
        connector.persist();
        return connector;
    }

    @Transactional
    public Connector updateConnectorDefaults(String connectorId,
                                            Boolean defaultPersistPipedocOrNull,
                                            Integer defaultMaxInlineSizeBytesOrNull,
                                            String defaultCustomConfigJsonOrNull,
                                            String displayNameOrNull,
                                            String ownerOrNull,
                                            String documentationUrlOrNull,
                                            String[] tagsOrNull) {
        Connector connector = findConnectorById(connectorId);
        if (connector == null) {
            return null;
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
            connector.persist();
        }

        return connector;
    }
}


