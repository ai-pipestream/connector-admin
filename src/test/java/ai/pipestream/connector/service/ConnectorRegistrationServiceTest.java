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
import io.quarkus.hibernate.reactive.panache.Panache;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.vertx.RunOnVertxContext;
import io.quarkus.test.vertx.UniAsserter;
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

    @BeforeEach
    @RunOnVertxContext
    void setUp(UniAsserter asserter) {
        // Clean up test data (reactive)
        asserter.execute(() -> Panache.withTransaction(() -> DataSource.deleteAll()));

        // Clear FK references before deleting schemas (reactive)
        asserter.execute(() -> Panache.withTransaction(() ->
            Connector.<Connector>listAll()
                .flatMap(connectors -> {
                    io.smallrye.mutiny.Uni<?> lastUni = io.smallrye.mutiny.Uni.createFrom().voidItem();
                    for (Connector connector : connectors) {
                        connector.customConfigSchemaId = null;
                        lastUni = connector.<Connector>persist();
                    }
                    return lastUni;
                })
        ));

        asserter.execute(() -> Panache.withTransaction(() ->
            DataSource.<DataSource>listAll()
                .flatMap(datasources -> {
                    io.smallrye.mutiny.Uni<?> lastUni = io.smallrye.mutiny.Uni.createFrom().voidItem();
                    for (DataSource datasource : datasources) {
                        datasource.customConfigSchemaId = null;
                        lastUni = datasource.<DataSource>persist();
                    }
                    return lastUni;
                })
        ));

        asserter.execute(() -> Panache.withTransaction(() -> ConnectorConfigSchema.deleteAll()));

        // Reset connector defaults for stable assertions (reactive)
        asserter.assertThat(() -> Panache.withTransaction(() ->
            Connector.<Connector>findById(TEST_CONNECTOR_ID)
                .flatMap(c -> {
                    assertNotNull(c, "Expected pre-seeded connector to exist");
                    c.customConfigSchemaId = null;
                    c.defaultPersistPipedoc = true;
                    c.defaultMaxInlineSizeBytes = 1048576;
                    c.defaultCustomConfig = "{}";
                    c.displayName = null;
                    c.owner = null;
                    c.documentationUrl = null;
                    c.tags = null;
                    return c.<Connector>persist();
                })
        ), connector -> {
            assertNotNull(connector);
        });
    }

    @Test
    @RunOnVertxContext
    void testCreateGetListDeleteSchema_Success(UniAsserter asserter) throws Exception {
        Struct customSchema = jsonToStruct("{\"type\":\"object\",\"properties\":{\"foo\":{\"type\":\"string\"}}}");
        Struct nodeSchema = jsonToStruct("{\"type\":\"object\",\"properties\":{\"desired_collection\":{\"type\":\"string\"}}}");

        // Create schema
        asserter.assertThat(() -> connectorRegistrationService.createConnectorConfigSchema(
            CreateConnectorConfigSchemaRequest.newBuilder()
                .setConnectorId(TEST_CONNECTOR_ID)
                .setSchemaVersion("v1")
                .setCustomConfigSchema(customSchema)
                .setNodeCustomConfigSchema(nodeSchema)
                .setCreatedBy("test")
                .build()
        ), create -> {
            assertTrue(create.getSuccess());
            assertNotNull(create.getSchema());
            assertFalse(create.getSchema().getSchemaId().isBlank());
            assertEquals(TEST_CONNECTOR_ID, create.getSchema().getConnectorId());
            assertEquals("v1", create.getSchema().getSchemaVersion());

            String schemaId = create.getSchema().getSchemaId();

            // Get schema
            asserter.assertThat(() -> connectorRegistrationService.getConnectorConfigSchema(
                GetConnectorConfigSchemaRequest.newBuilder()
                    .setSchemaId(schemaId)
                    .build()
            ), get -> {
                assertEquals(schemaId, get.getSchema().getSchemaId());
                assertEquals("v1", get.getSchema().getSchemaVersion());
                assertTrue(get.getSchema().hasCustomConfigSchema());
            });

            // List schemas
            asserter.assertThat(() -> connectorRegistrationService.listConnectorConfigSchemas(
                ListConnectorConfigSchemasRequest.newBuilder()
                    .setConnectorId(TEST_CONNECTOR_ID)
                    .setPageSize(50)
                    .build()
            ), list -> {
                assertEquals(1, list.getTotalCount());
                assertEquals(1, list.getSchemasCount());
                assertEquals(schemaId, list.getSchemas(0).getSchemaId());
            });

            // Delete schema
            asserter.assertThat(() -> connectorRegistrationService.deleteConnectorConfigSchema(
                DeleteConnectorConfigSchemaRequest.newBuilder()
                    .setSchemaId(schemaId)
                    .build()
            ), delete -> {
                assertTrue(delete.getSuccess());
            });

            // After delete, list should be empty
            asserter.assertThat(() -> connectorRegistrationService.listConnectorConfigSchemas(
                ListConnectorConfigSchemasRequest.newBuilder()
                    .setConnectorId(TEST_CONNECTOR_ID)
                    .setPageSize(50)
                    .build()
            ), listAfter -> {
                assertEquals(0, listAfter.getTotalCount());
                assertEquals(0, listAfter.getSchemasCount());
            });
        });
    }

    @Test
    @RunOnVertxContext
    void testCreateSchema_UniqueConstraint_AlreadyExists(UniAsserter asserter) throws Exception {
        Struct schema = jsonToStruct("{\"type\":\"object\"}");

        // Create first schema
        asserter.assertThat(() -> connectorRegistrationService.createConnectorConfigSchema(
            CreateConnectorConfigSchemaRequest.newBuilder()
                .setConnectorId(TEST_CONNECTOR_ID)
                .setSchemaVersion("v1")
                .setCustomConfigSchema(schema)
                .setNodeCustomConfigSchema(schema)
                .build()
        ), create -> {
            assertTrue(create.getSuccess());
        });

        // Try to create duplicate - should fail
        asserter.assertThat(() -> connectorRegistrationService.createConnectorConfigSchema(
            CreateConnectorConfigSchemaRequest.newBuilder()
                .setConnectorId(TEST_CONNECTOR_ID)
                .setSchemaVersion("v1")
                .setCustomConfigSchema(schema)
                .setNodeCustomConfigSchema(schema)
                .build()
        ).onFailure().recoverWithUni(failure -> {
            // Expected to fail, return null to indicate failure
            return io.smallrye.mutiny.Uni.createFrom().nullItem();
        }), result -> {
            assertNull(result, "Expected ALREADY_EXISTS error");
        });
    }

    @Test
    @RunOnVertxContext
    void testSetConnectorCustomConfigSchema_AndGetConnectorType(UniAsserter asserter) throws Exception {
        Struct schema = jsonToStruct("{\"type\":\"object\"}");

        // Create schema
        asserter.assertThat(() -> connectorRegistrationService.createConnectorConfigSchema(
            CreateConnectorConfigSchemaRequest.newBuilder()
                .setConnectorId(TEST_CONNECTOR_ID)
                .setSchemaVersion("v-set")
                .setCustomConfigSchema(schema)
                .setNodeCustomConfigSchema(schema)
                .build()
        ), create -> {
            assertTrue(create.getSuccess());
            String schemaId = create.getSchema().getSchemaId();

            // Set schema on connector
            asserter.assertThat(() -> connectorRegistrationService.setConnectorCustomConfigSchema(
                SetConnectorCustomConfigSchemaRequest.newBuilder()
                    .setConnectorId(TEST_CONNECTOR_ID)
                    .setSchemaId(schemaId)
                    .build()
            ), set -> {
                assertTrue(set.getSuccess());
                assertEquals(schemaId, set.getConnector().getCustomConfigSchemaId());
            });

            // Get connector and verify
            asserter.assertThat(() -> dataSourceAdminService.getConnectorType(
                GetConnectorTypeRequest.newBuilder().setConnectorId(TEST_CONNECTOR_ID).build()
            ), getConnector -> {
                assertEquals(schemaId, getConnector.getConnector().getCustomConfigSchemaId());
            });
        });
    }

    @Test
    @RunOnVertxContext
    void testUpdateConnectorTypeDefaults(UniAsserter asserter) throws Exception {
        Struct defaultCustomConfig = jsonToStruct("{\"parse_images\":true}");

        asserter.assertThat(() -> connectorRegistrationService.updateConnectorTypeDefaults(
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
        ), update -> {
            assertTrue(update.getSuccess());
            assertEquals(false, update.getConnector().getDefaultPersistPipedoc());
            assertEquals(12345, update.getConnector().getDefaultMaxInlineSizeBytes());
            assertTrue(update.getConnector().hasDefaultCustomConfig());
            assertEquals("S3 Connector", update.getConnector().getDisplayName());
            assertEquals("pipestream", update.getConnector().getOwner());
            assertEquals("https://docs.example.invalid/s3", update.getConnector().getDocumentationUrl());
            assertEquals(2, update.getConnector().getTagsCount());

            // Verify via getConnectorType
            asserter.assertThat(() -> dataSourceAdminService.getConnectorType(
                GetConnectorTypeRequest.newBuilder().setConnectorId(TEST_CONNECTOR_ID).build()
            ), getConnector -> {
                assertEquals(false, getConnector.getConnector().getDefaultPersistPipedoc());
                assertEquals(12345, getConnector.getConnector().getDefaultMaxInlineSizeBytes());
            });
        });
    }

    @Test
    @RunOnVertxContext
    void testListConnectorConfigSchemas_Pagination(UniAsserter asserter) throws Exception {
        Struct schema = jsonToStruct("{\"type\":\"object\"}");

        // Create multiple schemas
        for (int i = 1; i <= 5; i++) {
            final int version = i;
            asserter.execute(() -> connectorRegistrationService.createConnectorConfigSchema(
                CreateConnectorConfigSchemaRequest.newBuilder()
                    .setConnectorId(TEST_CONNECTOR_ID)
                    .setSchemaVersion("v" + version)
                    .setCustomConfigSchema(schema)
                    .setNodeCustomConfigSchema(schema)
                    .build()
            ).replaceWithVoid());
        }

        // Test pagination with page_size=2
        asserter.assertThat(() -> connectorRegistrationService.listConnectorConfigSchemas(
            ListConnectorConfigSchemasRequest.newBuilder()
                .setConnectorId(TEST_CONNECTOR_ID)
                .setPageSize(2)
                .build()
        ), page1 -> {
            assertEquals(5, page1.getTotalCount());
            assertEquals(2, page1.getSchemasCount());
            assertNotNull(page1.getNextPageToken());
            assertFalse(page1.getNextPageToken().isEmpty());

            String pageToken1 = page1.getNextPageToken();

            // Get second page
            asserter.assertThat(() -> connectorRegistrationService.listConnectorConfigSchemas(
                ListConnectorConfigSchemasRequest.newBuilder()
                    .setConnectorId(TEST_CONNECTOR_ID)
                    .setPageSize(2)
                    .setPageToken(pageToken1)
                    .build()
            ), page2 -> {
                assertEquals(5, page2.getTotalCount());
                assertEquals(2, page2.getSchemasCount());
                assertNotNull(page2.getNextPageToken());
                assertFalse(page2.getNextPageToken().isEmpty());

                String pageToken2 = page2.getNextPageToken();

                // Get third page (should have 1 remaining)
                asserter.assertThat(() -> connectorRegistrationService.listConnectorConfigSchemas(
                    ListConnectorConfigSchemasRequest.newBuilder()
                        .setConnectorId(TEST_CONNECTOR_ID)
                        .setPageSize(2)
                        .setPageToken(pageToken2)
                        .build()
                ), page3 -> {
                    assertEquals(5, page3.getTotalCount());
                    assertEquals(1, page3.getSchemasCount());
                    assertTrue(page3.getNextPageToken().isEmpty()); // No more pages
                });
            });
        });
    }

    @Test
    @RunOnVertxContext
    void testDeleteConnectorConfigSchema_BlockedByConnectorReference(UniAsserter asserter) throws Exception {
        Struct schema = jsonToStruct("{\"type\":\"object\"}");

        // Create schema
        asserter.assertThat(() -> connectorRegistrationService.createConnectorConfigSchema(
            CreateConnectorConfigSchemaRequest.newBuilder()
                .setConnectorId(TEST_CONNECTOR_ID)
                .setSchemaVersion("v-delete-test")
                .setCustomConfigSchema(schema)
                .setNodeCustomConfigSchema(schema)
                .build()
        ), create -> {
            assertTrue(create.getSuccess());
            String schemaId = create.getSchema().getSchemaId();

            // Link schema to connector
            asserter.assertThat(() -> connectorRegistrationService.setConnectorCustomConfigSchema(
                SetConnectorCustomConfigSchemaRequest.newBuilder()
                    .setConnectorId(TEST_CONNECTOR_ID)
                    .setSchemaId(schemaId)
                    .build()
            ), set -> {
                assertTrue(set.getSuccess());
            });

            // Try to delete - should fail due to application-level FK check
            asserter.assertThat(() -> connectorRegistrationService.deleteConnectorConfigSchema(
                DeleteConnectorConfigSchemaRequest.newBuilder()
                    .setSchemaId(schemaId)
                    .build()
            ).onFailure().recoverWithUni(failure -> {
                // Expected to fail, return null
                return io.smallrye.mutiny.Uni.createFrom().nullItem();
            }), result -> {
                assertNull(result, "Expected delete to fail due to FK constraint");
            });
        });
    }

    @Test
    @RunOnVertxContext
    void testUpdateConnectorTypeDefaults_PartialUpdates(UniAsserter asserter) throws Exception {
        // First set some initial values
        asserter.assertThat(() -> connectorRegistrationService.updateConnectorTypeDefaults(
            UpdateConnectorTypeDefaultsRequest.newBuilder()
                .setConnectorId(TEST_CONNECTOR_ID)
                .setDefaultPersistPipedoc(false)
                .setDefaultMaxInlineSizeBytes(50000)
                .setDisplayName("Initial Name")
                .setOwner("initial-owner")
                .build()
        ), initialUpdate -> {
            assertTrue(initialUpdate.getSuccess());

            // Now do partial update - only change display_name and add tags, leave other fields unchanged
            asserter.assertThat(() -> connectorRegistrationService.updateConnectorTypeDefaults(
                UpdateConnectorTypeDefaultsRequest.newBuilder()
                    .setConnectorId(TEST_CONNECTOR_ID)
                    .setDisplayName("Updated Name")
                    .addTags("new-tag")
                    .build()
            ), partialUpdate -> {
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
            });
        });
    }

    private Struct jsonToStruct(String json) throws Exception {
        Struct.Builder builder = Struct.newBuilder();
        JsonFormat.parser().merge(json, builder);
        return builder.build();
    }
}
