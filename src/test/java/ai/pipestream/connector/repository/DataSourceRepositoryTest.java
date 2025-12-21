package ai.pipestream.connector.repository;

import ai.pipestream.connector.entity.Connector;
import ai.pipestream.connector.entity.DataSource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for DataSourceRepository CRUD operations.
 */
@QuarkusTest
public class DataSourceRepositoryTest {

    @Inject
    DataSourceRepository repository;

    private static final String TEST_CONNECTOR_ID = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"; // Pre-seeded S3 connector

    @BeforeEach
    @Transactional
    void setUp() {
        // Clean up test data
        DataSource.deleteAll();
    }

    @Test
    void testGenerateDatasourceId_Deterministic() {
        String accountId = "test-account-123";
        String connectorId = TEST_CONNECTOR_ID;

        String id1 = repository.generateDatasourceId(accountId, connectorId);
        String id2 = repository.generateDatasourceId(accountId, connectorId);

        // Should be deterministic
        assertEquals(id1, id2);

        // Should be a valid UUID
        assertDoesNotThrow(() -> UUID.fromString(id1));
    }

    @Test
    void testGenerateDatasourceId_DifferentInputs() {
        String id1 = repository.generateDatasourceId("account-1", TEST_CONNECTOR_ID);
        String id2 = repository.generateDatasourceId("account-2", TEST_CONNECTOR_ID);

        // Different accounts should produce different IDs
        assertNotEquals(id1, id2);
    }

    @Test
    @Transactional
    void testCreateDataSource_Success() {
        String accountId = "test-account-" + System.currentTimeMillis();
        String name = "Test DataSource";
        String apiKeyHash = "test-hash-123";
        String driveName = "test-drive";

        DataSource ds = repository.createDataSource(
            accountId, TEST_CONNECTOR_ID, name, apiKeyHash, driveName, "{\"key\":\"value\"}"
        );

        assertNotNull(ds);
        assertNotNull(ds.datasourceId);
        assertEquals(accountId, ds.accountId);
        assertEquals(TEST_CONNECTOR_ID, ds.connectorId);
        assertEquals(name, ds.name);
        assertEquals(apiKeyHash, ds.apiKeyHash);
        assertEquals(driveName, ds.driveName);
        assertTrue(ds.active);
        assertNotNull(ds.createdAt);
    }

    @Test
    @Transactional
    void testCreateDataSource_DuplicateFails() {
        String accountId = "test-account-" + System.currentTimeMillis();

        // Create first datasource
        DataSource ds1 = repository.createDataSource(
            accountId, TEST_CONNECTOR_ID, "First", "hash1", "drive1", null
        );
        assertNotNull(ds1);

        // Attempt to create duplicate
        DataSource ds2 = repository.createDataSource(
            accountId, TEST_CONNECTOR_ID, "Second", "hash2", "drive2", null
        );
        assertNull(ds2); // Should return null for duplicate
    }

    @Test
    @Transactional
    void testFindByDatasourceId() {
        String accountId = "test-account-" + System.currentTimeMillis();

        DataSource created = repository.createDataSource(
            accountId, TEST_CONNECTOR_ID, "Test", "hash", "drive", null
        );

        DataSource found = repository.findByDatasourceId(created.datasourceId);

        assertNotNull(found);
        assertEquals(created.datasourceId, found.datasourceId);
        assertEquals(accountId, found.accountId);
    }

    @Test
    @Transactional
    void testFindByAccountAndConnector() {
        String accountId = "test-account-" + System.currentTimeMillis();

        repository.createDataSource(
            accountId, TEST_CONNECTOR_ID, "Test", "hash", "drive", null
        );

        DataSource found = repository.findByAccountAndConnector(accountId, TEST_CONNECTOR_ID);

        assertNotNull(found);
        assertEquals(accountId, found.accountId);
        assertEquals(TEST_CONNECTOR_ID, found.connectorId);
    }

    @Test
    @Transactional
    void testListByAccount() {
        String accountId = "test-account-" + System.currentTimeMillis();
        String secondConnectorId = "b1ffc0aa-0d1c-5f09-cc7e-7cc0ce491b22"; // Pre-seeded file-crawler

        repository.createDataSource(accountId, TEST_CONNECTOR_ID, "DS1", "hash1", "drive1", null);
        repository.createDataSource(accountId, secondConnectorId, "DS2", "hash2", "drive2", null);

        List<DataSource> list = repository.listByAccount(accountId, false, 0, 0);

        assertEquals(2, list.size());
    }

    @Test
    @Transactional
    void testSetDataSourceStatus() {
        String accountId = "test-account-" + System.currentTimeMillis();

        DataSource ds = repository.createDataSource(
            accountId, TEST_CONNECTOR_ID, "Test", "hash", "drive", null
        );
        assertTrue(ds.active);

        // Disable
        boolean result = repository.setDataSourceStatus(ds.datasourceId, false, "test_disable");
        assertTrue(result);

        DataSource updated = repository.findByDatasourceId(ds.datasourceId);
        assertFalse(updated.active);
        assertEquals("test_disable", updated.statusReason);

        // Re-enable
        result = repository.setDataSourceStatus(ds.datasourceId, true, null);
        assertTrue(result);

        updated = repository.findByDatasourceId(ds.datasourceId);
        assertTrue(updated.active);
    }

    @Test
    @Transactional
    void testRotateApiKey() {
        String accountId = "test-account-" + System.currentTimeMillis();
        String originalHash = "original-hash";
        String newHash = "new-hash-after-rotation";

        DataSource ds = repository.createDataSource(
            accountId, TEST_CONNECTOR_ID, "Test", originalHash, "drive", null
        );

        boolean result = repository.rotateApiKey(ds.datasourceId, newHash);
        assertTrue(result);

        DataSource updated = repository.findByDatasourceId(ds.datasourceId);
        assertEquals(newHash, updated.apiKeyHash);
        assertNotNull(updated.lastRotatedAt);
    }

    @Test
    @Transactional
    void testDeleteDataSource_SoftDelete() {
        String accountId = "test-account-" + System.currentTimeMillis();

        DataSource ds = repository.createDataSource(
            accountId, TEST_CONNECTOR_ID, "Test", "hash", "drive", null
        );
        assertTrue(ds.active);

        boolean result = repository.deleteDataSource(ds.datasourceId, "test_deletion");
        assertTrue(result);

        DataSource updated = repository.findByDatasourceId(ds.datasourceId);
        assertFalse(updated.active);
        assertEquals("test_deletion", updated.statusReason);
    }

    @Test
    @Transactional
    void testUpdateDataSource() {
        String accountId = "test-account-" + System.currentTimeMillis();

        DataSource ds = repository.createDataSource(
            accountId, TEST_CONNECTOR_ID, "Original Name", "hash", "original-drive", null
        );

        DataSource updated = repository.updateDataSource(
            ds.datasourceId, "Updated Name", "{\"updated\":\"true\"}", "new-drive"
        );

        assertNotNull(updated);
        assertEquals("Updated Name", updated.name);
        assertEquals("new-drive", updated.driveName);
        assertTrue(updated.metadata.contains("updated"));
    }

    @Test
    @Transactional
    void testListConnectorTypes() {
        List<Connector> connectors = repository.listConnectorTypes();

        // Should have at least the pre-seeded connectors
        assertTrue(connectors.size() >= 2);

        // Check for S3 connector
        assertTrue(connectors.stream().anyMatch(c -> "s3".equals(c.connectorType)));
        // Check for file-crawler connector
        assertTrue(connectors.stream().anyMatch(c -> "file-crawler".equals(c.connectorType)));
    }

    @Test
    @Transactional
    void testFindConnectorById() {
        Connector connector = repository.findConnectorById(TEST_CONNECTOR_ID);

        assertNotNull(connector);
        assertEquals("s3", connector.connectorType);
        assertEquals("UNMANAGED", connector.managementType);
    }

    @Test
    @Transactional
    void testFindConnectorByType() {
        Connector connector = repository.findConnectorByType("s3");

        assertNotNull(connector);
        assertEquals(TEST_CONNECTOR_ID, connector.connectorId);
    }

    @Test
    @Transactional
    void testCountDataSources() {
        String accountId = "test-account-" + System.currentTimeMillis();
        String secondConnectorId = "b1ffc0aa-0d1c-5f09-cc7e-7cc0ce491b22";

        repository.createDataSource(accountId, TEST_CONNECTOR_ID, "DS1", "hash1", "drive1", null);
        repository.createDataSource(accountId, secondConnectorId, "DS2", "hash2", "drive2", null);

        long count = repository.countDataSources(accountId, false);
        assertEquals(2, count);

        // Disable one
        DataSource ds = repository.findByAccountAndConnector(accountId, TEST_CONNECTOR_ID);
        repository.setDataSourceStatus(ds.datasourceId, false, "test");

        count = repository.countDataSources(accountId, false);
        assertEquals(1, count);

        count = repository.countDataSources(accountId, true); // Include inactive
        assertEquals(2, count);
    }
}
