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

    /**
     * Default constructor for CDI proxying.
     */
    public ConnectorRegistrationRepository() {}

    /**
     * Find a connector configuration schema by its unique identifier.
     *
     * @param schemaId The unique schema identifier
     * @return The found schema entity, or null if not found
     */
    public ConnectorConfigSchema findSchemaById(String schemaId) {
        return ConnectorConfigSchema.findById(schemaId);
    }

    /**
     * Find a connector configuration schema by connector ID and version string.
     *
     * @param connectorId The ID of the connector type
     * @param schemaVersion The version string (e.g., "1.0.0")
     * @return The found schema entity, or null if not found
     */
    public ConnectorConfigSchema findSchemaByConnectorAndVersion(String connectorId, String schemaVersion) {
        return ConnectorConfigSchema
            .find("connectorId = ?1 AND schemaVersion = ?2", connectorId, schemaVersion)
            .firstResult();
    }

    /**
     * List all configuration schemas for a specific connector type with pagination.
     *
     * @param connectorId The ID of the connector type
     * @param limit Maximum number of schemas to return
     * @param offset Number of schemas to skip
     * @return List of connector configuration schemas
     */
    public List<ConnectorConfigSchema> listSchemas(String connectorId, int limit, int offset) {
        var query = ConnectorConfigSchema
            .<ConnectorConfigSchema>find("connectorId = ?1 ORDER BY createdAt DESC", connectorId);
        if (offset > 0 || limit > 0) {
            int pageSize = limit > 0 ? limit : 50;
            query = query.page(offset > 0 ? offset / Math.max(pageSize, 1) : 0, pageSize);
        }
        return query.list();
    }

    /**
     * Count the total number of configuration schemas for a specific connector type.
     *
     * @param connectorId The ID of the connector type
     * @return Total number of schemas found
     */
    public long countSchemas(String connectorId) {
        return ConnectorConfigSchema.count("connectorId", connectorId);
    }

    /**
     * Create and persist a new connector configuration schema.
     *
     * @param schemaId Optional unique identifier (generated if null or blank)
     * @param connectorId The ID of the connector type this schema belongs to
     * @param schemaVersion The version string for this schema
     * @param customConfigSchemaJson JSON Schema for connector-wide custom configuration
     * @param nodeCustomConfigSchemaJson JSON Schema for node-specific custom configuration
     * @param createdBy Identifier of the user or system that created the schema
     * @return The newly created and persisted schema entity
     */
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

    /**
     * Delete a connector configuration schema by ID.
     *
     * @param schemaId The unique identifier of the schema to delete
     * @return true if the schema was found and deleted, false otherwise
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

    /**
     * Check if a schema is currently referenced as the active schema for any connector type.
     *
     * @param schemaId The unique identifier of the schema
     * @return true if referenced by a connector, false otherwise
     */
    public boolean isSchemaReferencedByConnector(String schemaId) {
        return Connector.count("customConfigSchemaId", schemaId) > 0;
    }

    /**
     * Check if a schema is currently referenced by any datasource.
     *
     * @param schemaId The unique identifier of the schema
     * @return true if referenced by a datasource, false otherwise
     */
    public boolean isSchemaReferencedByAnyDataSource(String schemaId) {
        return DataSource.count("customConfigSchemaId", schemaId) > 0;
    }

    /**
     * Find a connector type by its unique identifier.
     *
     * @param connectorId The unique connector identifier
     * @return The found connector entity, or null if not found
     */
    public Connector findConnectorById(String connectorId) {
        return Connector.findById(connectorId);
    }

    /**
     * Find a connector type by its type name (e.g., "s3").
     *
     * @param connectorType The connector type name
     * @return The found connector entity, or null if not found
     */
    public Connector findConnectorByType(String connectorType) {
        return Connector.find("connectorType", connectorType).firstResult();
    }

    /**
     * Create and persist a new connector type.
     *
     * @param connectorType The connector type name (e.g., "s3")
     * @param name Human-readable name for the connector
     * @param description Brief description of the connector
     * @param managementType Either "MANAGED" or "UNMANAGED"
     * @param displayName Optional display name for UI
     * @param owner Optional owner identifier
     * @param documentationUrl Optional URL for documentation
     * @param tags Optional list of tags for categorization
     * @return The newly created and persisted connector entity
     */
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

    /**
     * Delete a connector type by ID.
     *
     * @param connectorId The unique identifier of the connector type to delete
     * @return true if the connector was found and deleted, false otherwise
     */
    @Transactional
    public boolean deleteConnector(String connectorId) {
        Connector connector = findConnectorById(connectorId);
        if (connector == null) {
            return false;
        }
        connector.delete();
        return true;
    }

    /**
     * Check if any datasources are currently using a specific connector type.
     *
     * @param connectorId The unique identifier of the connector type
     * @return true if one or more datasources reference this connector, false otherwise
     */
    public boolean hasDataSources(String connectorId) {
        return DataSource.count("connectorId", connectorId) > 0;
    }

    /**
     * Check if any configuration schemas are registered for a specific connector type.
     *
     * @param connectorId The unique identifier of the connector type
     * @return true if one or more schemas reference this connector, false otherwise
     */
    public boolean hasSchemas(String connectorId) {
        return ConnectorConfigSchema.count("connectorId", connectorId) > 0;
    }

    /**
     * Generate a deterministic UUID based on the connector type name.
     *
     * @param connectorType The connector type name
     * @return A deterministic UUID string
     */
    public String generateConnectorId(String connectorType) {
        return UUID.nameUUIDFromBytes(connectorType.getBytes(StandardCharsets.UTF_8)).toString();
    }

    /**
     * Set the active configuration schema for a connector type.
     *
     * @param connectorId The unique identifier of the connector type
     * @param schemaIdOrEmpty The ID of the schema to set, or null/blank to clear
     * @return The updated connector entity, or null if not found
     */
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

    /**
     * Update default configuration values and metadata for a connector type.
     *
     * @param connectorId The unique identifier of the connector type
     * @param defaultPersistPipedocOrNull New default for pipedoc persistence (optional)
     * @param defaultMaxInlineSizeBytesOrNull New default for max inline size (optional)
     * @param defaultCustomConfigJsonOrNull New default custom configuration JSON (optional)
     * @param displayNameOrNull New display name (optional)
     * @param ownerOrNull New owner identifier (optional)
     * @param documentationUrlOrNull New documentation URL (optional)
     * @param tagsOrNull New list of tags (optional)
     * @return The updated connector entity, or null if not found
     */
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
