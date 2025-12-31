package ai.pipestream.connector.service;

import ai.pipestream.connector.entity.Connector;
import ai.pipestream.connector.entity.ConnectorConfigSchema;
import ai.pipestream.connector.entity.DataSource;
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

    private Struct jsonToStruct(String json) throws Exception {
        Struct.Builder builder = Struct.newBuilder();
        JsonFormat.parser().merge(json, builder);
        return builder.build();
    }
}


