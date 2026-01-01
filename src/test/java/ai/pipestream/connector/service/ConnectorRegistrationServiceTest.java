package ai.pipestream.connector.service;

import ai.pipestream.connector.entity.Connector;
import ai.pipestream.connector.entity.ConnectorConfigSchema;
import ai.pipestream.connector.entity.DataSource;
import ai.pipestream.connector.intake.v1.CreateDataSourceRequest;
import ai.pipestream.connector.intake.v1.CreateConnectorConfigSchemaRequest;
import ai.pipestream.connector.intake.v1.CreateConnectorConfigSchemaResponse;
import ai.pipestream.connector.intake.v1.DeleteConnectorConfigSchemaRequest;
import ai.pipestream.connector.intake.v1.GetConnectorConfigSchemaRequest;
import ai.pipestream.connector.intake.v1.GetConnectorTypeRequest;
import ai.pipestream.connector.intake.v1.ListConnectorConfigSchemasRequest;
import ai.pipestream.connector.intake.v1.MutinyConnectorRegistrationServiceGrpc;
import ai.pipestream.connector.intake.v1.MutinyDataSourceAdminServiceGrpc;
import ai.pipestream.connector.intake.v1.SetConnectorCustomConfigSchemaRequest;
import ai.pipestream.connector.intake.v1.UpdateConnectorTypeDefaultsRequest;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * gRPC integration tests for ConnectorRegistrationService.
 */
@QuarkusTest
public class ConnectorRegistrationServiceTest {

    @GrpcClient
    MutinyConnectorRegistrationServiceGrpc.MutinyConnectorRegistrationServiceStub connectorRegistrationService;

    @GrpcClient
    MutinyDataSourceAdminServiceGrpc.MutinyDataSourceAdminServiceStub dataSourceAdminService;

    private static final String TEST_CONNECTOR_ID = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"; // Pre-seeded S3

    @jakarta.inject.Inject
    EntityManager entityManager;

    @BeforeEach
    @Transactional
    void setUp() {
        // Clean up test data
        DataSource.deleteAll();

        // Clear FK references before deleting schemas
        entityManager.createQuery("UPDATE Connector c SET c.customConfigSchemaId = null").executeUpdate();
        entityManager.createQuery("UPDATE DataSource d SET d.customConfigSchemaId = null").executeUpdate();
        ConnectorConfigSchema.deleteAll();

        // Reset connector defaults for stable assertions
        Connector connector = Connector.findById(TEST_CONNECTOR_ID);
        assertNotNull(connector, "Expected pre-seeded connector to exist");
        connector.customConfigSchemaId = null;
        connector.defaultPersistPipedoc = true;
        connector.defaultMaxInlineSizeBytes = 1048576;
        connector.defaultCustomConfig = "{}";
        connector.displayName = null;
        connector.owner = null;
        connector.documentationUrl = null;
        connector.tags = null;
        connector.persist();
    }

    @Test
    void testCreateGetListDeleteSchema_Success() throws Exception {
        Struct customSchema = jsonToStruct("{\"type\":\"object\",\"properties\":{\"foo\":{\"type\":\"string\"}}}");
        Struct nodeSchema = jsonToStruct("{\"type\":\"object\",\"properties\":{\"desired_collection\":{\"type\":\"string\"}}}");

        CreateConnectorConfigSchemaResponse create = connectorRegistrationService.createConnectorConfigSchema(
            CreateConnectorConfigSchemaRequest.newBuilder()
                .setConnectorId(TEST_CONNECTOR_ID)
                .setSchemaVersion("v1")
                .setCustomConfigSchema(customSchema)
                .setNodeCustomConfigSchema(nodeSchema)
                .setCreatedBy("test")
                .build()
        ).await().indefinitely();

        assertTrue(create.getSuccess());
        assertNotNull(create.getSchema());
        assertFalse(create.getSchema().getSchemaId().isBlank());
        assertEquals(TEST_CONNECTOR_ID, create.getSchema().getConnectorId());
        assertEquals("v1", create.getSchema().getSchemaVersion());

        var get = connectorRegistrationService.getConnectorConfigSchema(
            GetConnectorConfigSchemaRequest.newBuilder()
                .setSchemaId(create.getSchema().getSchemaId())
                .build()
        ).await().indefinitely();
        assertEquals(create.getSchema().getSchemaId(), get.getSchema().getSchemaId());
        assertEquals("v1", get.getSchema().getSchemaVersion());
        assertTrue(get.getSchema().hasCustomConfigSchema());

        var list = connectorRegistrationService.listConnectorConfigSchemas(
            ListConnectorConfigSchemasRequest.newBuilder()
                .setConnectorId(TEST_CONNECTOR_ID)
                .setPageSize(50)
                .build()
        ).await().indefinitely();
        assertEquals(1, list.getTotalCount());
        assertEquals(1, list.getSchemasCount());
        assertEquals(create.getSchema().getSchemaId(), list.getSchemas(0).getSchemaId());

        var delete = connectorRegistrationService.deleteConnectorConfigSchema(
            DeleteConnectorConfigSchemaRequest.newBuilder()
                .setSchemaId(create.getSchema().getSchemaId())
                .build()
        ).await().indefinitely();
        assertTrue(delete.getSuccess());

        // After delete, list should be empty
        var listAfter = connectorRegistrationService.listConnectorConfigSchemas(
            ListConnectorConfigSchemasRequest.newBuilder()
                .setConnectorId(TEST_CONNECTOR_ID)
                .setPageSize(50)
                .build()
        ).await().indefinitely();
        assertEquals(0, listAfter.getTotalCount());
        assertEquals(0, listAfter.getSchemasCount());
    }

    @Test
    void testCreateSchema_UniqueConstraint_AlreadyExists() throws Exception {
        Struct schema = jsonToStruct("{\"type\":\"object\"}");

        connectorRegistrationService.createConnectorConfigSchema(
            CreateConnectorConfigSchemaRequest.newBuilder()
                .setConnectorId(TEST_CONNECTOR_ID)
                .setSchemaVersion("v1")
                .setCustomConfigSchema(schema)
                .setNodeCustomConfigSchema(schema)
                .build()
        ).await().indefinitely();

        try {
            connectorRegistrationService.createConnectorConfigSchema(
                CreateConnectorConfigSchemaRequest.newBuilder()
                    .setConnectorId(TEST_CONNECTOR_ID)
                    .setSchemaVersion("v1")
                    .setCustomConfigSchema(schema)
                    .setNodeCustomConfigSchema(schema)
                    .build()
            ).await().indefinitely();
            fail("Expected ALREADY_EXISTS");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("ALREADY_EXISTS") || e.getMessage().contains("already exists"));
        }
    }

    @Test
    void testSetConnectorCustomConfigSchema_AndGetConnectorType() throws Exception {
        Struct schema = jsonToStruct("{\"type\":\"object\"}");

        var create = connectorRegistrationService.createConnectorConfigSchema(
            CreateConnectorConfigSchemaRequest.newBuilder()
                .setConnectorId(TEST_CONNECTOR_ID)
                .setSchemaVersion("v-set")
                .setCustomConfigSchema(schema)
                .setNodeCustomConfigSchema(schema)
                .build()
        ).await().indefinitely();

        var set = connectorRegistrationService.setConnectorCustomConfigSchema(
            SetConnectorCustomConfigSchemaRequest.newBuilder()
                .setConnectorId(TEST_CONNECTOR_ID)
                .setSchemaId(create.getSchema().getSchemaId())
                .build()
        ).await().indefinitely();

        assertTrue(set.getSuccess());
        assertEquals(create.getSchema().getSchemaId(), set.getConnector().getCustomConfigSchemaId());

        var getConnector = dataSourceAdminService.getConnectorType(
            GetConnectorTypeRequest.newBuilder().setConnectorId(TEST_CONNECTOR_ID).build()
        ).await().indefinitely();

        assertEquals(create.getSchema().getSchemaId(), getConnector.getConnector().getCustomConfigSchemaId());
    }

    @Test
    void testUpdateConnectorTypeDefaults() throws Exception {
        Struct defaultCustomConfig = jsonToStruct("{\"parse_images\":true}");

        var update = connectorRegistrationService.updateConnectorTypeDefaults(
            UpdateConnectorTypeDefaultsRequest.newBuilder()
                .setConnectorId(TEST_CONNECTOR_ID)
                .setDefaultPersistPipedoc(false)
                .setDefaultMaxInlineSizeBytes(12345)
                .setDefaultCustomConfig(defaultCustomConfig)
                .setDisplayName("S3 Connector")
                .setOwner("pipestream")
                .setDocumentationUrl("https://docs.example.invalid/s3")
                .addTags("storage")
                .addTags("s3")
                .build()
        ).await().indefinitely();

        assertTrue(update.getSuccess());
        assertEquals(false, update.getConnector().getDefaultPersistPipedoc());
        assertEquals(12345, update.getConnector().getDefaultMaxInlineSizeBytes());
        assertTrue(update.getConnector().hasDefaultCustomConfig());
        assertEquals("S3 Connector", update.getConnector().getDisplayName());
        assertEquals("pipestream", update.getConnector().getOwner());
        assertEquals("https://docs.example.invalid/s3", update.getConnector().getDocumentationUrl());
        assertEquals(2, update.getConnector().getTagsCount());

        var getConnector = dataSourceAdminService.getConnectorType(
            GetConnectorTypeRequest.newBuilder().setConnectorId(TEST_CONNECTOR_ID).build()
        ).await().indefinitely();
        assertEquals(false, getConnector.getConnector().getDefaultPersistPipedoc());
        assertEquals(12345, getConnector.getConnector().getDefaultMaxInlineSizeBytes());
    }

    @Test
    void testListConnectorConfigSchemas_Pagination() throws Exception {
        Struct schema = jsonToStruct("{\"type\":\"object\"}");

        // Create multiple schemas
        for (int i = 1; i <= 5; i++) {
            connectorRegistrationService.createConnectorConfigSchema(
                CreateConnectorConfigSchemaRequest.newBuilder()
                    .setConnectorId(TEST_CONNECTOR_ID)
                    .setSchemaVersion("v" + i)
                    .setCustomConfigSchema(schema)
                    .setNodeCustomConfigSchema(schema)
                    .build()
            ).await().indefinitely();
        }

        // Test pagination with page_size=2
        var page1 = connectorRegistrationService.listConnectorConfigSchemas(
            ListConnectorConfigSchemasRequest.newBuilder()
                .setConnectorId(TEST_CONNECTOR_ID)
                .setPageSize(2)
                .build()
        ).await().indefinitely();

        assertEquals(5, page1.getTotalCount());
        assertEquals(2, page1.getSchemasCount());
        assertNotNull(page1.getNextPageToken());
        assertFalse(page1.getNextPageToken().isEmpty());

        // Get second page
        var page2 = connectorRegistrationService.listConnectorConfigSchemas(
            ListConnectorConfigSchemasRequest.newBuilder()
                .setConnectorId(TEST_CONNECTOR_ID)
                .setPageSize(2)
                .setPageToken(page1.getNextPageToken())
                .build()
        ).await().indefinitely();

        assertEquals(5, page2.getTotalCount());
        assertEquals(2, page2.getSchemasCount());
        assertNotNull(page2.getNextPageToken());
        assertFalse(page2.getNextPageToken().isEmpty());

        // Get third page (should have 1 remaining)
        var page3 = connectorRegistrationService.listConnectorConfigSchemas(
            ListConnectorConfigSchemasRequest.newBuilder()
                .setConnectorId(TEST_CONNECTOR_ID)
                .setPageSize(2)
                .setPageToken(page2.getNextPageToken())
                .build()
        ).await().indefinitely();

        assertEquals(5, page3.getTotalCount());
        assertEquals(1, page3.getSchemasCount());
        assertTrue(page3.getNextPageToken().isEmpty()); // No more pages
    }

    @Test
    void testDeleteConnectorConfigSchema_BlockedByConnectorReference() throws Exception {
        Struct schema = jsonToStruct("{\"type\":\"object\"}");

        // Create schema
        var create = connectorRegistrationService.createConnectorConfigSchema(
            CreateConnectorConfigSchemaRequest.newBuilder()
                .setConnectorId(TEST_CONNECTOR_ID)
                .setSchemaVersion("v-delete-test")
                .setCustomConfigSchema(schema)
                .setNodeCustomConfigSchema(schema)
                .build()
        ).await().indefinitely();
        assertTrue(create.getSuccess());

        // Link schema to connector
        var set = connectorRegistrationService.setConnectorCustomConfigSchema(
            SetConnectorCustomConfigSchemaRequest.newBuilder()
                .setConnectorId(TEST_CONNECTOR_ID)
                .setSchemaId(create.getSchema().getSchemaId())
                .build()
        ).await().indefinitely();
        assertTrue(set.getSuccess());

        // Try to delete - should fail due to application-level FK check
        try {
            connectorRegistrationService.deleteConnectorConfigSchema(
                DeleteConnectorConfigSchemaRequest.newBuilder()
                    .setSchemaId(create.getSchema().getSchemaId())
                    .build()
            ).await().indefinitely();
            fail("Expected delete to fail due to FK constraint");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("referenced") ||
                      e.getMessage().contains("FAILED_PRECONDITION") ||
                      e.getMessage().contains("Schema is still referenced"));
        }
    }


    @Test
    void testUpdateConnectorTypeDefaults_PartialUpdates() throws Exception {
        // First set some initial values
        var initialUpdate = connectorRegistrationService.updateConnectorTypeDefaults(
            UpdateConnectorTypeDefaultsRequest.newBuilder()
                .setConnectorId(TEST_CONNECTOR_ID)
                .setDefaultPersistPipedoc(false)
                .setDefaultMaxInlineSizeBytes(50000)
                .setDisplayName("Initial Name")
                .setOwner("initial-owner")
                .build()
        ).await().indefinitely();
        assertTrue(initialUpdate.getSuccess());

        // Now do partial update - only change display_name and add tags, leave other fields unchanged
        var partialUpdate = connectorRegistrationService.updateConnectorTypeDefaults(
            UpdateConnectorTypeDefaultsRequest.newBuilder()
                .setConnectorId(TEST_CONNECTOR_ID)
                .setDisplayName("Updated Name")
                .addTags("new-tag")
                .build()
        ).await().indefinitely();

        assertTrue(partialUpdate.getSuccess());
        var connector = partialUpdate.getConnector();

        // Fields that should be unchanged (not in request)
        assertEquals(false, connector.getDefaultPersistPipedoc()); // Should remain false
        assertEquals(50000, connector.getDefaultMaxInlineSizeBytes()); // Should remain 50000
        assertEquals("initial-owner", connector.getOwner()); // Should remain "initial-owner"

        // Fields that should be updated
        assertEquals("Updated Name", connector.getDisplayName()); // Should be updated
        assertEquals(1, connector.getTagsCount()); // Should have 1 tag
        assertEquals("new-tag", connector.getTags(0)); // Should have the new tag
    }

    private Struct jsonToStruct(String json) throws Exception {
        Struct.Builder builder = Struct.newBuilder();
        JsonFormat.parser().merge(json, builder);
        return builder.build();
    }
}


