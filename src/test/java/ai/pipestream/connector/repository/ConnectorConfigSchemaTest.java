package ai.pipestream.connector.repository;

import ai.pipestream.connector.entity.Connector;
import ai.pipestream.connector.entity.ConnectorConfigSchema;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for ConnectorConfigSchema entity CRUD operations.
 */
@QuarkusTest
public class ConnectorConfigSchemaTest {

    @Inject
    DataSourceRepository repository;
    
    @Inject
    EntityManager entityManager;

    private static final String TEST_CONNECTOR_ID = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"; // Pre-seeded S3 connector

    @BeforeEach
    @Transactional
    void setUp() {
        // Clean up any test-created schema references from connectors
        // This prevents FK violations when schemas are cleaned up
        Connector.<Connector>listAll().forEach(connector -> {
            // Only clear schema references that look like test UUIDs (not pre-seeded)
            // For safety, we'll clear all and let tests set them if needed
            if (connector.customConfigSchemaId != null) {
                connector.customConfigSchemaId = null;
                connector.persist();
            }
        });
    }
    
    @AfterEach
    @Transactional
    void tearDown() {
        // Clean up test-created schemas after clearing references
        // Delete schemas created in this test run (they won't have connector references)
        // Use a query to find and delete schemas not referenced by any connector
        ConnectorConfigSchema.<ConnectorConfigSchema>listAll().forEach(schema -> {
            // Check if any connector references this schema
            long refCount = Connector.count("customConfigSchemaId", schema.schemaId);
            if (refCount == 0) {
                schema.delete();
            }
        });
    }

    @Test
    @Transactional
    void testCreateConnectorConfigSchema_Success() {
        String schemaId = UUID.randomUUID().toString();
        String schemaVersion = "1.0.0";
        String customConfigSchema = "{\"type\":\"object\",\"properties\":{\"parse_images\":{\"type\":\"boolean\"}}}";
        String nodeCustomConfigSchema = "{\"type\":\"object\",\"properties\":{\"node_specific\":{\"type\":\"string\"}}}";
        String createdBy = "test-user";

        ConnectorConfigSchema schema = new ConnectorConfigSchema(
            schemaId, TEST_CONNECTOR_ID, schemaVersion,
            customConfigSchema, nodeCustomConfigSchema, createdBy
        );
        schema.persist();

        assertNotNull(schema);
        assertEquals(schemaId, schema.schemaId);
        assertEquals(TEST_CONNECTOR_ID, schema.connectorId);
        assertEquals(schemaVersion, schema.schemaVersion);
        assertEquals(customConfigSchema, schema.customConfigSchema);
        assertEquals(nodeCustomConfigSchema, schema.nodeCustomConfigSchema);
        assertEquals(createdBy, schema.createdBy);
        assertEquals("PENDING", schema.syncStatus);
        assertNotNull(schema.createdAt);
    }

    @Test
    @Transactional
    void testCreateConnectorConfigSchema_UniqueConstraint() {
        String schemaVersion = "1.0.0";
        String customConfigSchema = "{\"type\":\"object\"}";
        String nodeCustomConfigSchema = "{\"type\":\"object\"}";

        ConnectorConfigSchema schema1 = new ConnectorConfigSchema(
            UUID.randomUUID().toString(), TEST_CONNECTOR_ID, schemaVersion,
            customConfigSchema, nodeCustomConfigSchema, "user1"
        );
        schema1.persist();
        entityManager.flush(); // Force constraint check

        // Attempt to create duplicate (same connector + version)
        ConnectorConfigSchema schema2 = new ConnectorConfigSchema(
            UUID.randomUUID().toString(), TEST_CONNECTOR_ID, schemaVersion,
            customConfigSchema, nodeCustomConfigSchema, "user2"
        );

        // Should fail due to unique constraint when flushed
        assertThrows(Exception.class, () -> {
            schema2.persist();
            entityManager.flush(); // Force constraint check
        });
    }

    @Test
    @Transactional
    void testFindConnectorConfigSchemaById() {
        String schemaId = UUID.randomUUID().toString();
        String schemaVersion = "1.0.0";

        ConnectorConfigSchema created = new ConnectorConfigSchema(
            schemaId, TEST_CONNECTOR_ID, schemaVersion,
            "{}", "{}", "test-user"
        );
        created.persist();

        ConnectorConfigSchema found = ConnectorConfigSchema.findById(schemaId);

        assertNotNull(found);
        assertEquals(schemaId, found.schemaId);
        assertEquals(TEST_CONNECTOR_ID, found.connectorId);
        assertEquals(schemaVersion, found.schemaVersion);
    }

    @Test
    @Transactional
    void testFindConnectorConfigSchemaByConnectorId() {
        String schemaVersion = "1.0.0";
        String secondConnectorId = "b1ffc0aa-0d1c-5f09-cc7e-7cc0ce491b22"; // Pre-seeded file-crawler

        ConnectorConfigSchema schema1 = new ConnectorConfigSchema(
            UUID.randomUUID().toString(), TEST_CONNECTOR_ID, schemaVersion,
            "{}", "{}", "user"
        );
        schema1.persist();

        ConnectorConfigSchema schema2 = new ConnectorConfigSchema(
            UUID.randomUUID().toString(), secondConnectorId, schemaVersion,
            "{}", "{}", "user"
        );
        schema2.persist();

        // Find by connector ID
        var found = ConnectorConfigSchema.<ConnectorConfigSchema>find("connectorId", TEST_CONNECTOR_ID).list();

        assertEquals(1, found.size());
        ConnectorConfigSchema foundSchema = found.get(0);
        assertEquals(TEST_CONNECTOR_ID, foundSchema.connectorId);
    }

    @Test
    @Transactional
    void testUpdateConnectorConfigSchema() {
        String schemaId = UUID.randomUUID().toString();
        String originalSchema = "{\"type\":\"object\",\"properties\":{\"old\":{\"type\":\"string\"}}}";
        String updatedSchema = "{\"type\":\"object\",\"properties\":{\"new\":{\"type\":\"boolean\"}}}";

        ConnectorConfigSchema schema = new ConnectorConfigSchema(
            schemaId, TEST_CONNECTOR_ID, "1.0.0",
            originalSchema, "{}", "test-user"
        );
        schema.persist();

        // Update schema
        schema.customConfigSchema = updatedSchema;
        schema.syncStatus = "SYNCED";
        schema.apicurioArtifactId = "test-artifact-123";
        schema.persist();

        ConnectorConfigSchema updated = ConnectorConfigSchema.findById(schemaId);
        assertNotNull(updated);
        assertEquals(updatedSchema, updated.customConfigSchema);
        assertEquals("SYNCED", updated.syncStatus);
        assertEquals("test-artifact-123", updated.apicurioArtifactId);
    }

    @Test
    @Transactional
    void testDeleteConnectorConfigSchema() {
        String schemaId = UUID.randomUUID().toString();

        ConnectorConfigSchema schema = new ConnectorConfigSchema(
            schemaId, TEST_CONNECTOR_ID, "1.0.0",
            "{}", "{}", "test-user"
        );
        schema.persist();
        entityManager.flush();

        assertNotNull(ConnectorConfigSchema.findById(schemaId));

        // Explicit deletion should work when no connector references this schema
        schema.delete();
        entityManager.flush();

        assertNull(ConnectorConfigSchema.findById(schemaId));
    }

    @Test
    @Transactional
    void testConnector_CustomConfigSchemaId_FkConstraintRequiresSchemaExists() {
        Connector connector = repository.findConnectorById(TEST_CONNECTOR_ID);
        assertNotNull(connector);

        String nonexistentSchemaId = UUID.randomUUID().toString();
        connector.customConfigSchemaId = nonexistentSchemaId;
        connector.persist();

        // Should fail on flush due to FK (connectors.custom_config_schema_id -> connector_config_schemas.schema_id)
        assertThrows(Exception.class, () -> entityManager.flush());
    }

    @Test
    @Transactional
    void testConnectorConfigSchema_RelationshipToConnector() {
        String schemaId = UUID.randomUUID().toString();

        ConnectorConfigSchema schema = new ConnectorConfigSchema(
            schemaId, TEST_CONNECTOR_ID, "1.0.0",
            "{}", "{}", "test-user"
        );
        schema.persist();

        // Verify connector relationship exists (lazy loaded)
        Connector connector = repository.findConnectorById(TEST_CONNECTOR_ID);
        assertNotNull(connector);
        assertEquals(TEST_CONNECTOR_ID, connector.connectorId);
        
        // Verify schema can be loaded
        ConnectorConfigSchema loadedSchema = ConnectorConfigSchema.findById(schemaId);
        assertNotNull(loadedSchema);
        assertEquals(TEST_CONNECTOR_ID, loadedSchema.connectorId);
    }

    @Test
    @Transactional
    void testConnectorConfigSchema_ApicurioSyncFields() {
        String schemaId = UUID.randomUUID().toString();

        ConnectorConfigSchema schema = new ConnectorConfigSchema(
            schemaId, TEST_CONNECTOR_ID, "1.0.0",
            "{}", "{}", "test-user"
        );
        schema.persist();

        // Update Apicurio sync fields
        schema.apicurioArtifactId = "my-artifact-id";
        schema.apicurioGlobalId = 12345L;
        schema.syncStatus = "SYNCED";
        schema.lastSyncAttempt = OffsetDateTime.now();
        schema.persist();

        ConnectorConfigSchema updated = ConnectorConfigSchema.findById(schemaId);
        assertNotNull(updated);
        assertEquals("my-artifact-id", updated.apicurioArtifactId);
        assertEquals(12345L, updated.apicurioGlobalId);
        assertEquals("SYNCED", updated.syncStatus);
        assertNotNull(updated.lastSyncAttempt);
    }

    @Test
    @Transactional
    void testConnectorConfigSchema_UpdateSyncError() {
        String schemaId = UUID.randomUUID().toString();

        ConnectorConfigSchema schema = new ConnectorConfigSchema(
            schemaId, TEST_CONNECTOR_ID, "1.0.0",
            "{}", "{}", "test-user"
        );
        schema.persist();

        // Simulate sync failure
        schema.syncStatus = "FAILED";
        schema.syncError = "Connection timeout";
        schema.lastSyncAttempt = OffsetDateTime.now();
        schema.persist();

        ConnectorConfigSchema updated = ConnectorConfigSchema.findById(schemaId);
        assertNotNull(updated);
        assertEquals("FAILED", updated.syncStatus);
        assertEquals("Connection timeout", updated.syncError);
        assertNotNull(updated.lastSyncAttempt);
    }

    @Test
    @Transactional
    void testConnectorConfigSchema_MultipleVersions() {
        String schemaId1 = UUID.randomUUID().toString();
        String schemaId2 = UUID.randomUUID().toString();

        // Create version 1.0.0
        ConnectorConfigSchema schema1 = new ConnectorConfigSchema(
            schemaId1, TEST_CONNECTOR_ID, "1.0.0",
            "{}", "{}", "user"
        );
        schema1.persist();

        // Create version 2.0.0 for same connector
        ConnectorConfigSchema schema2 = new ConnectorConfigSchema(
            schemaId2, TEST_CONNECTOR_ID, "2.0.0",
            "{}", "{}", "user"
        );
        schema2.persist();

        // Both should exist
        assertNotNull(ConnectorConfigSchema.findById(schemaId1));
        assertNotNull(ConnectorConfigSchema.findById(schemaId2));

        // Find all versions for connector
        var allVersions = ConnectorConfigSchema.find("connectorId", TEST_CONNECTOR_ID).list();
        assertEquals(2, allVersions.size());
    }
}

