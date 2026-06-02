package ai.pipestream.connector.repository;

import ai.pipestream.connector.entity.Connector;
import ai.pipestream.connector.entity.DataSource;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.transaction.Transactional;
import org.jboss.logging.Logger;

import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

/**
 * Repository for DataSource entity CRUD operations.
 *
 * <p>All methods use Hibernate ORM Panache synchronously. Service methods call
 * this repository from Quarkus gRPC worker threads, so database access remains
 * straightforward and does not expose Mutiny to connector-admin business code.
 */
@ApplicationScoped
public class DataSourceRepository {

    private static final Logger LOG = Logger.getLogger(DataSourceRepository.class);

    /**
     * Default constructor for CDI proxying.
     */
    public DataSourceRepository() {}

    /**
     * Generate a deterministic datasource identifier based on account and connector IDs.
     *
     * @param accountId The unique account identifier
     * @param connectorId The unique connector identifier
     * @return A deterministic UUID string
     */
    public String generateDatasourceId(String accountId, String connectorId) {
        String input = accountId + ":" + connectorId;
        return UUID.nameUUIDFromBytes(input.getBytes(StandardCharsets.UTF_8)).toString();
    }

    /**
     * Create and persist a new datasource.
     *
     * @param accountId The account that owns the datasource
     * @param connectorId The connector type ID to bind to
     * @param name Human-readable name for the datasource
     * @param apiKeyHash Hashed API key for authentication
     * @param driveName Optional drive or storage identifier
     * @param metadata Optional metadata JSON or description
     * @return The newly created datasource entity, or null if it already exists
     */
    @Transactional
    public DataSource createDataSource(String accountId, String connectorId, String name,
                                       String apiKeyHash, String driveName, String metadata) {
        String datasourceId = generateDatasourceId(accountId, connectorId);
        DataSource existing = DataSource.findById(datasourceId);
        if (existing != null) {
            LOG.warnf("DataSource already exists for account %s and connector %s", accountId, connectorId);
            return null;
        }

        DataSource datasource = new DataSource(datasourceId, accountId, connectorId, name, apiKeyHash, driveName);
        if (metadata != null && !metadata.isEmpty()) {
            datasource.metadata = metadata;
        }

        LOG.infof("Creating datasource: %s (account: %s, connector: %s)", datasourceId, accountId, connectorId);
        datasource.persist();
        return datasource;
    }

    /**
     * Find a datasource by its unique identifier.
     *
     * @param datasourceId The unique datasource identifier
     * @return The found datasource entity, or null if not found
     */
    public DataSource findByDatasourceId(String datasourceId) {
        return DataSource.findById(datasourceId);
    }

    /**
     * Find a datasource by account and connector type identifiers.
     *
     * @param accountId The unique account identifier
     * @param connectorId The unique connector identifier
     * @return The found datasource entity, or null if not found
     */
    public DataSource findByAccountAndConnector(String accountId, String connectorId) {
        return findByDatasourceId(generateDatasourceId(accountId, connectorId));
    }

    /**
     * List datasources for a specific account with pagination.
     *
     * @param accountId The unique account identifier
     * @param includeInactive Whether to include inactive (soft-deleted) datasources
     * @param limit Maximum number of datasources to return
     * @param offset Number of datasources to skip
     * @return List of datasource entities
     */
    public List<DataSource> listByAccount(String accountId, boolean includeInactive, int limit, int offset) {
        var query = includeInactive
            ? DataSource.<DataSource>find("accountId = ?1 ORDER BY createdAt DESC", accountId)
            : DataSource.<DataSource>find("accountId = ?1 AND active = true ORDER BY createdAt DESC", accountId);
        return page(query, limit, offset).list();
    }

    /**
     * List all datasources in the system with pagination.
     *
     * @param includeInactive Whether to include inactive (soft-deleted) datasources
     * @param limit Maximum number of datasources to return
     * @param offset Number of datasources to skip
     * @return List of datasource entities
     */
    public List<DataSource> listAll(boolean includeInactive, int limit, int offset) {
        var query = includeInactive
            ? DataSource.<DataSource>findAll()
            : DataSource.<DataSource>find("active = true");
        return page(query, limit, offset).list();
    }

    /**
     * Count total datasources, optionally filtered by account.
     *
     * @param accountId Optional account identifier to filter by
     * @param includeInactive Whether to include inactive (soft-deleted) datasources in count
     * @return Total count of datasources
     */
    public long countDataSources(String accountId, boolean includeInactive) {
        if (accountId != null && !accountId.isEmpty()) {
            return includeInactive
                ? DataSource.count("accountId", accountId)
                : DataSource.count("accountId = ?1 AND active = true", accountId);
        }
        return includeInactive ? DataSource.count() : DataSource.count("active = true");
    }

    /**
     * Update an existing datasource's metadata and settings.
     *
     * @param datasourceId The unique datasource identifier
     * @param newName New display name (optional)
     * @param newMetadata New metadata JSON (optional)
     * @param newDriveName New drive or storage identifier (optional)
     * @return The updated datasource entity, or null if not found
     */
    @Transactional
    public DataSource updateDataSource(String datasourceId, String newName, String newMetadata, String newDriveName) {
        DataSource datasource = DataSource.findById(datasourceId);
        if (datasource == null) {
            LOG.warnf("Cannot update datasource - not found: %s", datasourceId);
            return null;
        }

        boolean changed = false;
        if (newName != null && !newName.isEmpty()) {
            datasource.name = newName;
            changed = true;
        }
        if (newMetadata != null && !newMetadata.isEmpty()) {
            datasource.metadata = newMetadata;
            changed = true;
        }
        if (newDriveName != null && !newDriveName.isEmpty()) {
            datasource.driveName = newDriveName;
            changed = true;
        }
        if (changed) {
            datasource.updatedAt = OffsetDateTime.now();
        }
        return datasource;
    }

    /**
     * Activate or deactivate a datasource.
     *
     * @param datasourceId The unique datasource identifier
     * @param active true to activate, false to deactivate
     * @param reason The reason for the status change
     * @return true if status was updated or already at target, false if not found
     */
    @Transactional
    public boolean setDataSourceStatus(String datasourceId, boolean active, String reason) {
        DataSource datasource = DataSource.findById(datasourceId);
        if (datasource == null) {
            LOG.warnf("Cannot set status - datasource not found: %s", datasourceId);
            return false;
        }
        if (Boolean.valueOf(active).equals(datasource.active)) {
            return true;
        }
        datasource.active = active;
        datasource.statusReason = reason;
        datasource.updatedAt = OffsetDateTime.now();
        return true;
    }

    /**
     * Soft-delete a datasource by setting active=false.
     *
     * @param datasourceId The unique datasource identifier
     * @param reason The reason for deletion
     * @return true if deleted, false if not found
     */
    @Transactional
    public boolean deleteDataSource(String datasourceId, String reason) {
        DataSource datasource = DataSource.findById(datasourceId);
        if (datasource == null) {
            LOG.warnf("Cannot delete datasource - not found: %s", datasourceId);
            return false;
        }
        datasource.active = false;
        datasource.statusReason = reason;
        datasource.updatedAt = OffsetDateTime.now();
        return true;
    }

    /**
     * Update the hashed API key for a datasource.
     *
     * @param datasourceId The unique datasource identifier
     * @param newApiKeyHash The new hashed API key
     * @return true if rotated, false if not found
     */
    @Transactional
    public boolean rotateApiKey(String datasourceId, String newApiKeyHash) {
        DataSource datasource = DataSource.findById(datasourceId);
        if (datasource == null) {
            LOG.warnf("Cannot rotate API key - datasource not found: %s", datasourceId);
            return false;
        }
        datasource.apiKeyHash = newApiKeyHash;
        datasource.lastRotatedAt = OffsetDateTime.now();
        datasource.updatedAt = OffsetDateTime.now();
        return true;
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
     * Find a connector type by its type name.
     *
     * @param connectorType The connector type name (e.g., "s3")
     * @return The found connector entity, or null if not found
     */
    public Connector findConnectorByType(String connectorType) {
        return Connector.find("connectorType", connectorType).firstResult();
    }

    /**
     * List all available connector types.
     *
     * @return List of connector entities
     */
    public List<Connector> listConnectorTypes() {
        return Connector.listAll();
    }

    /**
     * Count total available connector types.
     *
     * @return Total count of connector types
     */
    public long countConnectorTypes() {
        return Connector.count();
    }

    private static <T> io.quarkus.hibernate.orm.panache.PanacheQuery<T> page(
            io.quarkus.hibernate.orm.panache.PanacheQuery<T> query, int limit, int offset) {
        if (offset > 0 || limit > 0) {
            int pageSize = limit > 0 ? limit : 50;
            int pageIndex = offset > 0 ? offset / Math.max(pageSize, 1) : 0;
            return query.page(pageIndex, pageSize);
        }
        return query;
    }
}
