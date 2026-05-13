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

    public String generateDatasourceId(String accountId, String connectorId) {
        String input = accountId + ":" + connectorId;
        return UUID.nameUUIDFromBytes(input.getBytes(StandardCharsets.UTF_8)).toString();
    }

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

    public DataSource findByDatasourceId(String datasourceId) {
        return DataSource.findById(datasourceId);
    }

    public DataSource findByAccountAndConnector(String accountId, String connectorId) {
        return findByDatasourceId(generateDatasourceId(accountId, connectorId));
    }

    public List<DataSource> listByAccount(String accountId, boolean includeInactive, int limit, int offset) {
        var query = includeInactive
            ? DataSource.<DataSource>find("accountId = ?1 ORDER BY createdAt DESC", accountId)
            : DataSource.<DataSource>find("accountId = ?1 AND active = true ORDER BY createdAt DESC", accountId);
        return page(query, limit, offset).list();
    }

    public List<DataSource> listAll(boolean includeInactive, int limit, int offset) {
        var query = includeInactive
            ? DataSource.<DataSource>findAll()
            : DataSource.<DataSource>find("active = true");
        return page(query, limit, offset).list();
    }

    public long countDataSources(String accountId, boolean includeInactive) {
        if (accountId != null && !accountId.isEmpty()) {
            return includeInactive
                ? DataSource.count("accountId", accountId)
                : DataSource.count("accountId = ?1 AND active = true", accountId);
        }
        return includeInactive ? DataSource.count() : DataSource.count("active = true");
    }

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

    public Connector findConnectorById(String connectorId) {
        return Connector.findById(connectorId);
    }

    public Connector findConnectorByType(String connectorType) {
        return Connector.find("connectorType", connectorType).firstResult();
    }

    public List<Connector> listConnectorTypes() {
        return Connector.listAll();
    }

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
