package ai.pipestream.connector.repository;

import ai.pipestream.connector.entity.Connector;
import ai.pipestream.connector.entity.DataSource;
import io.quarkus.hibernate.reactive.panache.Panache;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.vertx.RunOnVertxContext;
import io.quarkus.test.vertx.UniAsserter;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for DataSourceRepository CRUD operations with Hibernate Reactive.
 */
@QuarkusTest
public class DataSourceRepositoryTest {

    @Inject
    DataSourceRepository repository;

    private static final String TEST_CONNECTOR_ID = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"; // Pre-seeded S3 connector

    @BeforeEach
    @RunOnVertxContext
    void setUp(UniAsserter asserter) {
        // Clean up test data (reactive)
        asserter.execute(() -> Panache.withTransaction(() -> DataSource.deleteAll()));
    }

    @Test
    void testGenerateDatasourceId_Deterministic() {
        // These are synchronous methods, no need for reactive context
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
        // These are synchronous methods, no need for reactive context
        String id1 = repository.generateDatasourceId("account-1", TEST_CONNECTOR_ID);
        String id2 = repository.generateDatasourceId("account-2", TEST_CONNECTOR_ID);

        // Different accounts should produce different IDs
        assertNotEquals(id1, id2);
    }

    @Test
    void testGenerateDatasourceId_UsesColonSeparator() {
        // Test that the ID generation uses accountId + ":" + connectorId as specified
        String accountId = "test-account";
        String connectorId = "test-connector";

        String generatedId = repository.generateDatasourceId(accountId, connectorId);

        // Manually compute what the ID should be with colon separator
        String expectedInput = accountId + ":" + connectorId;
        String expectedId = UUID.nameUUIDFromBytes(expectedInput.getBytes(java.nio.charset.StandardCharsets.UTF_8)).toString();

        // Verify the generated ID matches the expected ID (with colon separator)
        assertEquals(expectedId, generatedId);

        // Also verify it's different from ID without colon separator
        String wrongInput = accountId + connectorId; // no colon
        String wrongId = UUID.nameUUIDFromBytes(wrongInput.getBytes(java.nio.charset.StandardCharsets.UTF_8)).toString();
        assertNotEquals(wrongId, generatedId);
    }

    @Test
    @RunOnVertxContext
    void testCreateDataSource_Success(UniAsserter asserter) {
        String accountId = "test-account-" + System.currentTimeMillis();
        String name = "Test DataSource";
        String apiKeyHash = "test-hash-123";
        String driveName = "test-drive";

        asserter.assertThat(() -> Panache.withTransaction(() -> repository.createDataSource(
            accountId, TEST_CONNECTOR_ID, name, apiKeyHash, driveName, "{\"key\":\"value\"}"
        )), ds -> {
            assertNotNull(ds);
            assertNotNull(ds.datasourceId);
            assertEquals(accountId, ds.accountId);
            assertEquals(TEST_CONNECTOR_ID, ds.connectorId);
            assertEquals(name, ds.name);
            assertEquals(apiKeyHash, ds.apiKeyHash);
            assertEquals(driveName, ds.driveName);
            assertTrue(ds.active);
            assertNotNull(ds.createdAt);
        });
    }

    @Test
    @RunOnVertxContext
    void testCreateDataSource_DuplicateFails(UniAsserter asserter) {
        String accountId = "test-account-" + System.currentTimeMillis();

        // Create first datasource
        asserter.assertThat(() -> Panache.withTransaction(() -> repository.createDataSource(
            accountId, TEST_CONNECTOR_ID, "First", "hash1", "drive1", null
        )), ds1 -> {
            assertNotNull(ds1, "First datasource should be created");
        });

        // Attempt to create duplicate
        asserter.assertThat(() -> Panache.withTransaction(() -> repository.createDataSource(
            accountId, TEST_CONNECTOR_ID, "Second", "hash2", "drive2", null
        )), ds2 -> {
            assertNull(ds2, "Duplicate datasource should return null");
        });
    }

    @Test
    @RunOnVertxContext
    void testFindByDatasourceId(UniAsserter asserter) {
        String accountId = "test-account-" + System.currentTimeMillis();

        asserter.assertThat(() -> Panache.withTransaction(() -> repository.createDataSource(
            accountId, TEST_CONNECTOR_ID, "Test", "hash", "drive", null
        )), created -> {
            assertNotNull(created);
            String datasourceId = created.datasourceId;
            // Now find it by ID
            asserter.assertThat(() -> Panache.withSession(() -> repository.findByDatasourceId(datasourceId)), found -> {
                assertNotNull(found);
                assertEquals(datasourceId, found.datasourceId);
                assertEquals(accountId, found.accountId);
            });
        });
    }

    @Test
    @RunOnVertxContext
    void testFindByAccountAndConnector(UniAsserter asserter) {
        String accountId = "test-account-" + System.currentTimeMillis();

        asserter.execute(() -> Panache.withTransaction(() -> repository.createDataSource(
            accountId, TEST_CONNECTOR_ID, "Test", "hash", "drive", null
        ).replaceWithVoid()));

        asserter.assertThat(() -> Panache.withSession(() -> repository.findByAccountAndConnector(accountId, TEST_CONNECTOR_ID)), found -> {
            assertNotNull(found);
            assertEquals(accountId, found.accountId);
            assertEquals(TEST_CONNECTOR_ID, found.connectorId);
        });
    }

    @Test
    @RunOnVertxContext
    void testListByAccount(UniAsserter asserter) {
        String accountId = "test-account-" + System.currentTimeMillis();
        String secondConnectorId = "b1ffc0aa-0d1c-5f09-cc7e-7cc0ce491b22"; // Pre-seeded file-crawler

        // Create first datasource
        asserter.execute(() -> Panache.withTransaction(() -> 
            repository.createDataSource(accountId, TEST_CONNECTOR_ID, "DS1", "hash1", "drive1", null)
                .replaceWithVoid()
        ));
        
        // Create second datasource
        asserter.execute(() -> Panache.withTransaction(() -> 
            repository.createDataSource(accountId, secondConnectorId, "DS2", "hash2", "drive2", null)
                .replaceWithVoid()
        ));

        // List all datasources for account
        asserter.assertThat(() -> Panache.withSession(() -> 
            repository.listByAccount(accountId, false, 0, 0)
        ), list -> {
            assertEquals(2, list.size(), "Should find 2 datasources for account");
        });
    }

    @Test
    @RunOnVertxContext
    void testSetDataSourceStatus(UniAsserter asserter) {
        String accountId = "test-account-" + System.currentTimeMillis();

        asserter.assertThat(() -> Panache.withTransaction(() -> repository.createDataSource(
            accountId, TEST_CONNECTOR_ID, "Test", "hash", "drive", null
        )), ds -> {
            assertTrue(ds.active);
            String datasourceId = ds.datasourceId;

            // Disable
            asserter.assertThat(() -> Panache.withTransaction(() -> 
                repository.setDataSourceStatus(datasourceId, false, "test_disable")
            ), result -> {
                assertTrue(result);
            });

            // Verify disabled
            asserter.assertThat(() -> Panache.withSession(() -> 
                repository.findByDatasourceId(datasourceId)
            ), updated -> {
                assertFalse(updated.active);
                assertEquals("test_disable", updated.statusReason);
            });

            // Re-enable
            asserter.assertThat(() -> Panache.withTransaction(() -> 
                repository.setDataSourceStatus(datasourceId, true, null)
            ), result -> {
                assertTrue(result);
            });

            // Verify enabled
            asserter.assertThat(() -> Panache.withSession(() -> 
                repository.findByDatasourceId(datasourceId)
            ), updated -> {
                assertTrue(updated.active);
            });
        });
    }

    @Test
    @RunOnVertxContext
    void testRotateApiKey(UniAsserter asserter) {
        String accountId = "test-account-" + System.currentTimeMillis();
        String originalHash = "original-hash";
        String newHash = "new-hash-after-rotation";

        asserter.assertThat(() -> Panache.withTransaction(() -> repository.createDataSource(
            accountId, TEST_CONNECTOR_ID, "Test", originalHash, "drive", null
        )), ds -> {
            String datasourceId = ds.datasourceId;

            // Rotate key
            asserter.assertThat(() -> Panache.withTransaction(() -> 
                repository.rotateApiKey(datasourceId, newHash)
            ), result -> {
                assertTrue(result);
            });

            // Verify rotation
            asserter.assertThat(() -> Panache.withSession(() -> 
                repository.findByDatasourceId(datasourceId)
            ), updated -> {
                assertEquals(newHash, updated.apiKeyHash);
                assertNotNull(updated.lastRotatedAt);
            });
        });
    }

    @Test
    @RunOnVertxContext
    void testDeleteDataSource_SoftDelete(UniAsserter asserter) {
        String accountId = "test-account-" + System.currentTimeMillis();

        asserter.assertThat(() -> Panache.withTransaction(() -> repository.createDataSource(
            accountId, TEST_CONNECTOR_ID, "Test", "hash", "drive", null
        )), ds -> {
            assertTrue(ds.active);
            String datasourceId = ds.datasourceId;

            // Soft delete
            asserter.assertThat(() -> Panache.withTransaction(() -> 
                repository.deleteDataSource(datasourceId, "test_deletion")
            ), result -> {
                assertTrue(result);
            });

            // Verify soft delete
            asserter.assertThat(() -> Panache.withSession(() -> 
                repository.findByDatasourceId(datasourceId)
            ), updated -> {
                assertFalse(updated.active);
                assertEquals("test_deletion", updated.statusReason);
            });
        });
    }

    @Test
    @RunOnVertxContext
    void testUpdateDataSource(UniAsserter asserter) {
        String accountId = "test-account-" + System.currentTimeMillis();

        asserter.assertThat(() -> Panache.withTransaction(() -> repository.createDataSource(
            accountId, TEST_CONNECTOR_ID, "Original Name", "hash", "original-drive", null
        )), ds -> {
            String datasourceId = ds.datasourceId;

            // Update datasource
            asserter.assertThat(() -> Panache.withTransaction(() -> repository.updateDataSource(
                datasourceId, "Updated Name", "{\"updated\":\"true\"}", "new-drive"
            )), updated -> {
                assertNotNull(updated);
                assertEquals("Updated Name", updated.name);
                assertEquals("new-drive", updated.driveName);
                assertTrue(updated.metadata.contains("updated"));
            });
        });
    }

    @Test
    @RunOnVertxContext
    void testListConnectorTypes(UniAsserter asserter) {
        asserter.assertThat(() -> Panache.withSession(() -> repository.listConnectorTypes()), connectors -> {
            // Should have at least the pre-seeded connectors
            assertTrue(connectors.size() >= 2);
            // Check for S3 connector
            assertTrue(connectors.stream().anyMatch(c -> "s3".equals(c.connectorType)));
            // Check for file-crawler connector
            assertTrue(connectors.stream().anyMatch(c -> "file-crawler".equals(c.connectorType)));
        });
    }

    @Test
    @RunOnVertxContext
    void testFindConnectorById(UniAsserter asserter) {
        asserter.assertThat(() -> Panache.withSession(() -> repository.findConnectorById(TEST_CONNECTOR_ID)), connector -> {
            assertNotNull(connector);
            assertEquals("s3", connector.connectorType);
            assertEquals("UNMANAGED", connector.managementType);
        });
    }

    @Test
    @RunOnVertxContext
    void testFindConnectorByType(UniAsserter asserter) {
        asserter.assertThat(() -> Panache.withSession(() -> repository.findConnectorByType("s3")), connector -> {
            assertNotNull(connector);
            assertEquals(TEST_CONNECTOR_ID, connector.connectorId);
        });
    }

    @Test
    @RunOnVertxContext
    void testCountDataSources(UniAsserter asserter) {
        String accountId = "test-account-" + System.currentTimeMillis();
        String secondConnectorId = "b1ffc0aa-0d1c-5f09-cc7e-7cc0ce491b22";

        // Create two datasources
        asserter.execute(() -> Panache.withTransaction(() -> 
            repository.createDataSource(accountId, TEST_CONNECTOR_ID, "DS1", "hash1", "drive1", null)
                .replaceWithVoid()
        ));
        asserter.execute(() -> Panache.withTransaction(() -> 
            repository.createDataSource(accountId, secondConnectorId, "DS2", "hash2", "drive2", null)
                .replaceWithVoid()
        ));

        // Count active
        asserter.assertThat(() -> Panache.withSession(() -> repository.countDataSources(accountId, false)), count -> {
            assertEquals(2, count);
        });

        // Disable one - find it first, then disable
        String[] datasourceIdHolder = new String[1];
        asserter.assertThat(() -> Panache.withSession(() -> repository.findByAccountAndConnector(accountId, TEST_CONNECTOR_ID)), ds -> {
            datasourceIdHolder[0] = ds.datasourceId;
        });
        
        // Now disable it
        asserter.execute(() -> Panache.withTransaction(() -> 
            repository.setDataSourceStatus(datasourceIdHolder[0], false, "test").replaceWithVoid()
        ));

        // Count active again (should be 1)
        asserter.assertThat(() -> Panache.withSession(() -> repository.countDataSources(accountId, false)), count -> {
            assertEquals(1, count);
        });

        // Count all (including inactive, should be 2)
        asserter.assertThat(() -> Panache.withSession(() -> repository.countDataSources(accountId, true)), count -> {
            assertEquals(2, count);
        });
    }
}
