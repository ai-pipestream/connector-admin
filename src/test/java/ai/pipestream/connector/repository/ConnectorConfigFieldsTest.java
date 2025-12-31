package ai.pipestream.connector.repository;

import ai.pipestream.connector.entity.Connector;
import ai.pipestream.connector.entity.ConnectorConfigSchema;
import ai.pipestream.connector.entity.DataSource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for new configuration fields in Connector and DataSource entities.
 */
@QuarkusTest
public class ConnectorConfigFieldsTest {

    @Inject
    DataSourceRepository repository;

    private static final String TEST_CONNECTOR_ID = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"; // Pre-seeded S3 connector

    @BeforeEach
    @Transactional
    void setUp() {
        // Clean up test datasources
        DataSource.deleteAll();
        
        // Clear any test-created schema references from connectors and datasources
        Connector.<Connector>listAll().forEach(connector -> {
            if (connector.customConfigSchemaId != null) {
                connector.customConfigSchemaId = null;
                connector.persist();
            }
        });
    }
    
    @AfterEach
    @Transactional
    void tearDown() {
        // Clean up test-created schemas (those not referenced by any connector/datasource)
        ConnectorConfigSchema.<ConnectorConfigSchema>listAll().forEach(schema -> {
            long connectorRefs = Connector.count("customConfigSchemaId", schema.schemaId);
            long datasourceRefs = DataSource.count("customConfigSchemaId", schema.schemaId);
            if (connectorRefs == 0 && datasourceRefs == 0) {
                schema.delete();
            }
        });
    }

    // ========================================================================
    // Connector Configuration Fields Tests
    // ========================================================================

    @Test
    @Transactional
    void testConnector_DefaultConfigurationFields() {
        Connector connector = repository.findConnectorById(TEST_CONNECTOR_ID);
        assertNotNull(connector);

        // New fields should be nullable/optional (may be null for existing connectors)
        // This test just verifies we can read and write them
        connector.defaultPersistPipedoc = false;
        connector.defaultMaxInlineSizeBytes = 2097152; // 2MB
        connector.defaultCustomConfig = "{\"test\":\"value\"}";
        connector.displayName = "S3 Connector";
        connector.owner = "pipestream-team";
        connector.documentationUrl = "https://docs.example.com/s3";
        connector.tags = new String[]{"storage", "s3", "aws"};
        connector.persist();

        Connector updated = repository.findConnectorById(TEST_CONNECTOR_ID);
        assertNotNull(updated);
        assertFalse(updated.defaultPersistPipedoc);
        assertEquals(2097152, updated.defaultMaxInlineSizeBytes);
        assertEquals("{\"test\":\"value\"}", updated.defaultCustomConfig);
        assertEquals("S3 Connector", updated.displayName);
        assertEquals("pipestream-team", updated.owner);
        assertEquals("https://docs.example.com/s3", updated.documentationUrl);
        assertArrayEquals(new String[]{"storage", "s3", "aws"}, updated.tags);
    }

    @Test
    @Transactional
    void testConnector_ConfigSchemaRelationship() {
        // Create a config schema
        String schemaId = UUID.randomUUID().toString();
        ConnectorConfigSchema schema = new ConnectorConfigSchema(
            schemaId, TEST_CONNECTOR_ID, "1.0.0",
            "{}", "{}", "test-user"
        );
        schema.persist();

        // Link connector to schema
        Connector connector = repository.findConnectorById(TEST_CONNECTOR_ID);
        assertNotNull(connector);
        connector.customConfigSchemaId = schemaId;
        connector.persist();

        // Verify relationship
        Connector updated = repository.findConnectorById(TEST_CONNECTOR_ID);
        assertNotNull(updated);
        assertEquals(schemaId, updated.customConfigSchemaId);
    }

    // ========================================================================
    // DataSource Configuration Fields Tests
    // ========================================================================

    @Test
    @Transactional
    void testDataSource_GlobalConfigProto() {
        String accountId = "test-account-" + System.currentTimeMillis();
        DataSource ds = repository.createDataSource(
            accountId, TEST_CONNECTOR_ID, "Test", "hash", "drive", null
        );

        // Set global config proto (simulated serialized protobuf)
        byte[] configProto = "test-proto-bytes".getBytes();
        ds.globalConfigProto = configProto;
        ds.persist();

        DataSource updated = repository.findByDatasourceId(ds.datasourceId);
        assertNotNull(updated);
        assertNotNull(updated.globalConfigProto);
        assertArrayEquals(configProto, updated.globalConfigProto);
    }

    @Test
    @Transactional
    void testDataSource_CustomConfig() {
        String accountId = "test-account-" + System.currentTimeMillis();
        DataSource ds = repository.createDataSource(
            accountId, TEST_CONNECTOR_ID, "Test", "hash", "drive", null
        );

        // Set custom config (JSON Schema-validated)
        String customConfig = "{\"parse_images\":true,\"include_history\":false}";
        ds.customConfig = customConfig;
        ds.persist();

        DataSource updated = repository.findByDatasourceId(ds.datasourceId);
        assertNotNull(updated);
        assertEquals(customConfig, updated.customConfig);
    }

    @Test
    @Transactional
    void testDataSource_ConfigSchemaReference() {
        // Create a config schema
        String schemaId = UUID.randomUUID().toString();
        ConnectorConfigSchema schema = new ConnectorConfigSchema(
            schemaId, TEST_CONNECTOR_ID, "1.0.0",
            "{}", "{}", "test-user"
        );
        schema.persist();

        // Create datasource with schema reference
        String accountId = "test-account-" + System.currentTimeMillis();
        DataSource ds = repository.createDataSource(
            accountId, TEST_CONNECTOR_ID, "Test", "hash", "drive", null
        );
        ds.customConfigSchemaId = schemaId;
        ds.persist();

        DataSource updated = repository.findByDatasourceId(ds.datasourceId);
        assertNotNull(updated);
        assertEquals(schemaId, updated.customConfigSchemaId);
    }

    @Test
    @Transactional
    void testDataSource_AllConfigFieldsTogether() {
        // Create schema
        String schemaId = UUID.randomUUID().toString();
        ConnectorConfigSchema schema = new ConnectorConfigSchema(
            schemaId, TEST_CONNECTOR_ID, "1.0.0",
            "{\"type\":\"object\"}", "{\"type\":\"object\"}", "test-user"
        );
        schema.persist();

        // Create datasource with all config fields
        String accountId = "test-account-" + System.currentTimeMillis();
        DataSource ds = repository.createDataSource(
            accountId, TEST_CONNECTOR_ID, "Test", "hash", "drive", null
        );

        byte[] configProto = "global-config-proto".getBytes();
        String customConfig = "{\"connector_specific\":\"value\"}";

        ds.globalConfigProto = configProto;
        ds.customConfig = customConfig;
        ds.customConfigSchemaId = schemaId;
        ds.persist();

        DataSource updated = repository.findByDatasourceId(ds.datasourceId);
        assertNotNull(updated);
        assertArrayEquals(configProto, updated.globalConfigProto);
        assertEquals(customConfig, updated.customConfig);
        assertEquals(schemaId, updated.customConfigSchemaId);
    }

    @Test
    @Transactional
    void testDataSource_ConfigFieldsNullable() {
        // Verify that config fields can be null (for backward compatibility)
        String accountId = "test-account-" + System.currentTimeMillis();
        DataSource ds = repository.createDataSource(
            accountId, TEST_CONNECTOR_ID, "Test", "hash", "drive", null
        );

        // All new config fields should be null by default
        assertNull(ds.globalConfigProto);
        assertNull(ds.customConfig);
        assertNull(ds.customConfigSchemaId);

        // Verify it persists correctly with null values
        ds.persist();
        DataSource updated = repository.findByDatasourceId(ds.datasourceId);
        assertNotNull(updated);
        assertNull(updated.globalConfigProto);
        assertNull(updated.customConfig);
        assertNull(updated.customConfigSchemaId);
    }

    @Test
    @Transactional
    void testConnector_DefaultValues() {
        Connector connector = repository.findConnectorById(TEST_CONNECTOR_ID);
        assertNotNull(connector);

        // Verify default values when explicitly set
        connector.defaultPersistPipedoc = null; // Clear existing
        connector.defaultMaxInlineSizeBytes = null;
        connector.persist();

        Connector updated = repository.findConnectorById(TEST_CONNECTOR_ID);
        assertNotNull(updated);
        // Defaults are set at application level, not DB level
        // This test verifies fields can be null
        assertNull(updated.defaultPersistPipedoc);
        assertNull(updated.defaultMaxInlineSizeBytes);
    }
}

