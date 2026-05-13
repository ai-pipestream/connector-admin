package ai.pipestream.connector.repository;

import ai.pipestream.connector.entity.Connector;
import ai.pipestream.connector.entity.ConnectorConfigSchema;
import ai.pipestream.connector.entity.DataSource;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.transaction.Transactional;
import org.jboss.logging.Logger;

import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

/**
 * Repository for connector registration operations using Hibernate ORM Panache.
 */
@ApplicationScoped
public class ConnectorRegistrationRepository {

    private static final Logger LOG = Logger.getLogger(ConnectorRegistrationRepository.class);

    public ConnectorConfigSchema findSchemaById(String schemaId) {
        return ConnectorConfigSchema.findById(schemaId);
    }

    public ConnectorConfigSchema findSchemaByConnectorAndVersion(String connectorId, String schemaVersion) {
        return ConnectorConfigSchema
            .find("connectorId = ?1 AND schemaVersion = ?2", connectorId, schemaVersion)
            .firstResult();
    }

    public List<ConnectorConfigSchema> listSchemas(String connectorId, int limit, int offset) {
        var query = ConnectorConfigSchema
            .<ConnectorConfigSchema>find("connectorId = ?1 ORDER BY createdAt DESC", connectorId);
        if (offset > 0 || limit > 0) {
            int pageSize = limit > 0 ? limit : 50;
            query = query.page(offset > 0 ? offset / Math.max(pageSize, 1) : 0, pageSize);
        }
        return query.list();
    }

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

        LOG.infof("Creating connector config schema %s for connector %s version %s", id, connectorId, schemaVersion);
        schema.persist();
        return schema;
    }

    @Transactional
    public boolean deleteSchema(String schemaId) {
        ConnectorConfigSchema schema = findSchemaById(schemaId);
        if (schema == null) {
            return false;
        }
        schema.delete();
        return true;
    }

    public boolean isSchemaReferencedByConnector(String schemaId) {
        return Connector.count("customConfigSchemaId", schemaId) > 0;
    }

    public boolean isSchemaReferencedByAnyDataSource(String schemaId) {
        return DataSource.count("customConfigSchemaId", schemaId) > 0;
    }

    public Connector findConnectorById(String connectorId) {
        return Connector.findById(connectorId);
    }

    public Connector findConnectorByType(String connectorType) {
        return Connector.find("connectorType", connectorType).firstResult();
    }

    @Transactional
    public Connector createConnector(String connectorType, String name, String description,
                                     String managementType, String displayName, String owner,
                                     String documentationUrl, List<String> tags) {
        String connectorId = generateConnectorId(connectorType);

        Connector connector = new Connector(connectorId, connectorType, name, description, managementType);
        if (displayName != null && !displayName.isBlank()) {
            connector.displayName = displayName;
        }
        if (owner != null && !owner.isBlank()) {
            connector.owner = owner;
        }
        if (documentationUrl != null && !documentationUrl.isBlank()) {
            connector.documentationUrl = documentationUrl;
        }
        if (tags != null && !tags.isEmpty()) {
            connector.tags = tags;
        }

        LOG.infof("Creating connector type '%s' with id %s", connectorType, connectorId);
        connector.persist();
        return connector;
    }

    @Transactional
    public boolean deleteConnector(String connectorId) {
        Connector connector = findConnectorById(connectorId);
        if (connector == null) {
            return false;
        }
        connector.delete();
        return true;
    }

    public boolean hasDataSources(String connectorId) {
        return DataSource.count("connectorId", connectorId) > 0;
    }

    public boolean hasSchemas(String connectorId) {
        return ConnectorConfigSchema.count("connectorId", connectorId) > 0;
    }

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
                                             List<String> tagsOrNull) {
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
        }
        return connector;
    }
}
