package ai.pipestream.connector.service;

import ai.pipestream.connector.intake.v1.CreateDataSourceRequest;
import ai.pipestream.connector.intake.v1.CreateDataSourceResponse;
import ai.pipestream.connector.intake.v1.DeleteDataSourceRequest;
import ai.pipestream.connector.intake.v1.GetDataSourceRequest;
import ai.pipestream.connector.intake.v1.GetDataSourceResponse;
import ai.pipestream.connector.intake.v1.ListConnectorTypesRequest;
import ai.pipestream.connector.intake.v1.ListConnectorTypesResponse;
import ai.pipestream.connector.intake.v1.ListDataSourcesRequest;
import ai.pipestream.connector.intake.v1.ListDataSourcesResponse;
import ai.pipestream.connector.intake.v1.MutinyDataSourceAdminServiceGrpc;
import ai.pipestream.connector.intake.v1.RotateApiKeyRequest;
import ai.pipestream.connector.intake.v1.RotateApiKeyResponse;
import ai.pipestream.connector.intake.v1.SetDataSourceStatusRequest;
import ai.pipestream.connector.intake.v1.SetDataSourceStatusResponse;
import ai.pipestream.connector.intake.v1.UpdateDataSourceRequest;
import ai.pipestream.connector.intake.v1.ValidateApiKeyRequest;
import ai.pipestream.connector.intake.v1.ValidateApiKeyResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.net.URL;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Integration tests for the full DataSource document-upload lifecycle.
 *
 * <p>Runs against the production JAR via {@link QuarkusIntegrationTest} — no CDI
 * injection, no mocked beans.  A real PostgreSQL container is started by the
 * framework (Testcontainers) and Flyway migrates the schema before the tests run.
 *
 * <p><strong>What this test demonstrates:</strong>
 * <ol>
 *   <li>Discovering available connector types via {@code ListConnectorTypes}.</li>
 *   <li>Creating a DataSource and receiving a one-time plaintext API key.</li>
 *   <li>Validating the API key — this is exactly what the
 *       {@code connector-intake} service calls each time a connector submits a
 *       document for ingestion.  A {@code valid=true} response means the upload
 *       is authorised; the returned {@code DataSourceConfig} is used by the
 *       intake service to route and process the document.</li>
 *   <li>Reading back the DataSource and confirming the API key is never re-exposed.</li>
 *   <li>Listing DataSources for an account.</li>
 *   <li>Updating DataSource metadata.</li>
 *   <li>Rotating the API key and confirming the old key is invalidated.</li>
 *   <li>Disabling and re-enabling the DataSource (maintenance-window simulation).</li>
 *   <li>Soft-deleting the DataSource.</li>
 * </ol>
 *
 * <p>Account validation is exercised through pipestream-wiremock-server, not
 * connector-admin's in-process test stub.
 */
@QuarkusIntegrationTest
@QuarkusTestResource(ConnectorAdminIntegrationTestResource.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DataSourceLifecycleIT {

    /** Pre-seeded S3 connector ID (Flyway V1 migration). */
    private static final String S3_CONNECTOR_ID = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11";

    /** Active account seeded by pipestream-wiremock-server's AccountManagerMock. */
    private static final String TEST_ACCOUNT_ID = "valid-account";

    @TestHTTPResource
    URL url;

    private ManagedChannel channel;
    private MutinyDataSourceAdminServiceGrpc.MutinyDataSourceAdminServiceStub stub;

    /** Shared state threaded through ordered test methods. */
    private static String datasourceId;
    private static String currentApiKey;

    @BeforeEach
    void setUp() {
        channel = ManagedChannelBuilder
                .forAddress(url.getHost(), url.getPort())
                .usePlaintext()
                .build();
        stub = MutinyDataSourceAdminServiceGrpc.newMutinyStub(channel);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        if (channel != null) {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    // =========================================================================
    // 1. Connector type discovery
    // =========================================================================

    /**
     * Verifies that the pre-seeded connector types are present.
     * A connector type must exist before a DataSource can be created for it.
     */
    @Test
    @Order(1)
    void step1_listConnectorTypes_containsPreseededTypes() {
        ListConnectorTypesResponse response = stub
                .listConnectorTypes(ListConnectorTypesRequest.newBuilder().build())
                .await().indefinitely();

        assertThat(response.getConnectorsList())
                .as("pre-seeded connector types should include 's3'")
                .anyMatch(c -> "s3".equals(c.getConnectorType()));
        assertThat(response.getTotalCount())
                .as("should have at least one connector type")
                .isGreaterThanOrEqualTo(1);
    }

    @Test
    @Order(2)
    void step2_createDataSource_rejectsInactiveAccount() {
        StatusRuntimeException ex = assertThrows(StatusRuntimeException.class, () ->
                stub.createDataSource(
                        CreateDataSourceRequest.newBuilder()
                                .setAccountId("inactive-account")
                                .setConnectorId(S3_CONNECTOR_ID)
                                .setName("Inactive Account DataSource")
                                .setDriveName("it-test-drive")
                                .build()
                ).await().indefinitely()
        );

        assertThat(ex.getStatus().getCode())
                .as("inactive accounts must not be allowed to create datasources")
                .isEqualTo(Status.Code.INVALID_ARGUMENT);
    }

    @Test
    @Order(3)
    void step3_createDataSource_rejectsMissingAccount() {
        StatusRuntimeException ex = assertThrows(StatusRuntimeException.class, () ->
                stub.createDataSource(
                        CreateDataSourceRequest.newBuilder()
                                .setAccountId("nonexistent")
                                .setConnectorId(S3_CONNECTOR_ID)
                                .setName("Missing Account DataSource")
                                .setDriveName("it-test-drive")
                                .build()
                ).await().indefinitely()
        );

        assertThat(ex.getStatus().getCode())
                .as("missing accounts must not be allowed to create datasources")
                .isEqualTo(Status.Code.INVALID_ARGUMENT);
    }

    // =========================================================================
    // 2. DataSource creation — API key is returned once
    // =========================================================================

    /**
     * Creates a DataSource for the test account and captures the one-time
     * plaintext API key that a connector will use to authenticate uploads.
     *
     * <p>The API key is generated as a 256-bit cryptographically secure random
     * string and stored as an Argon2id hash; the plaintext is returned only here.
     */
    @Test
    @Order(4)
    void step4_createDataSource_returnsOneTimeApiKey() {
        CreateDataSourceResponse response = stub.createDataSource(
                CreateDataSourceRequest.newBuilder()
                        .setAccountId(TEST_ACCOUNT_ID)
                        .setConnectorId(S3_CONNECTOR_ID)
                        .setName("E2E Lifecycle Test — S3 DataSource")
                        .setDriveName("it-test-drive")
                        .putMetadata("purpose", "integration-test")
                        .putMetadata("connector", "s3")
                        .build()
        ).await().indefinitely();

        assertThat(response.getSuccess()).as("creation should succeed").isTrue();
        assertThat(response.getDatasource().getDatasourceId())
                .as("datasource_id should be generated")
                .isNotBlank();
        assertThat(response.getDatasource().getApiKey())
                .as("api_key should be returned on creation (plaintext, one-time)")
                .isNotBlank();
        assertThat(response.getDatasource().getActive())
                .as("new datasource should be active")
                .isTrue();
        assertThat(response.getDatasource().getAccountId())
                .isEqualTo(TEST_ACCOUNT_ID);
        assertThat(response.getDatasource().getConnectorId())
                .isEqualTo(S3_CONNECTOR_ID);

        // Store for subsequent tests
        datasourceId = response.getDatasource().getDatasourceId();
        currentApiKey = response.getDatasource().getApiKey();
    }

    // =========================================================================
    // 3. API key validation — simulates what the intake service calls
    // =========================================================================

    /**
     * Validates the API key returned by {@link #step4_createDataSource_returnsOneTimeApiKey}.
     *
     * <p>This is the exact call that the {@code connector-intake} service makes when a
     * connector submits a document for ingestion.  A {@code valid=true} response means
     * the upload is authorised.  The response also contains a {@code DataSourceConfig}
     * that the intake service uses to route the document (drive name, persistence policy,
     * hydration policy, etc.).
     */
    @Test
    @Order(5)
    void step5_validateApiKey_authorisesDocumentUpload() {
        ValidateApiKeyResponse response = stub.validateApiKey(
                ValidateApiKeyRequest.newBuilder()
                        .setDatasourceId(datasourceId)
                        .setApiKey(currentApiKey)
                        .build()
        ).await().indefinitely();

        assertThat(response.getValid())
                .as("API key should be valid — upload is authorised")
                .isTrue();
        assertThat(response.getMessage())
                .as("message should confirm validity")
                .containsIgnoringCase("valid");

        // DataSourceConfig is returned to the intake service for routing decisions
        assertThat(response.getConfig().getDatasourceId())
                .as("config should reference the correct datasource")
                .isEqualTo(datasourceId);
        assertThat(response.getConfig().getDriveName())
                .as("drive name should be carried through")
                .isEqualTo("it-test-drive");
        assertThat(response.getConfig().hasGlobalConfig())
                .as("merged Tier-1 config should be present")
                .isTrue();
        assertThat(response.getConfig().getGlobalConfig().hasPersistenceConfig())
                .as("persistence config should contain system defaults")
                .isTrue();
    }

    /**
     * Confirms that a wrong API key is rejected, preventing unauthorised uploads.
     */
    @Test
    @Order(6)
    void step6_validateApiKey_wrongKey_rejectsUpload() {
        ValidateApiKeyResponse response = stub.validateApiKey(
                ValidateApiKeyRequest.newBuilder()
                        .setDatasourceId(datasourceId)
                        .setApiKey("not-the-real-key")
                        .build()
        ).await().indefinitely();

        assertThat(response.getValid())
                .as("wrong API key should be rejected")
                .isFalse();
    }

    // =========================================================================
    // 4. GetDataSource — API key is NOT re-exposed
    // =========================================================================

    /**
     * Reads back the DataSource and confirms the API key is never returned after
     * the initial creation response — callers cannot recover the plaintext key.
     */
    @Test
    @Order(7)
    void step7_getDataSource_doesNotExposeApiKey() {
        GetDataSourceResponse response = stub.getDataSource(
                GetDataSourceRequest.newBuilder()
                        .setDatasourceId(datasourceId)
                        .build()
        ).await().indefinitely();

        assertThat(response.getDatasource().getDatasourceId()).isEqualTo(datasourceId);
        assertThat(response.getDatasource().getApiKey())
                .as("API key must not be exposed after initial creation")
                .isEmpty();
        assertThat(response.getDatasource().getActive()).isTrue();
    }

    // =========================================================================
    // 5. List DataSources for account
    // =========================================================================

    /**
     * Verifies that the newly created DataSource appears in the account's list.
     */
    @Test
    @Order(8)
    void step8_listDataSources_showsCreatedDatasource() {
        ListDataSourcesResponse response = stub.listDataSources(
                ListDataSourcesRequest.newBuilder()
                        .setAccountId(TEST_ACCOUNT_ID)
                        .build()
        ).await().indefinitely();

        assertThat(response.getDatasourcesList())
                .as("account datasource list should contain the created datasource")
                .anyMatch(ds -> ds.getDatasourceId().equals(datasourceId));
        assertThat(response.getTotalCount()).isGreaterThanOrEqualTo(1);
    }

    // =========================================================================
    // 6. Update DataSource
    // =========================================================================

    /**
     * Updates the DataSource name and metadata and confirms the changes persist.
     */
    @Test
    @Order(9)
    void step9_updateDataSource_persistsChanges() {
        stub.updateDataSource(
                UpdateDataSourceRequest.newBuilder()
                        .setDatasourceId(datasourceId)
                        .setName("E2E Lifecycle Test — Updated")
                        .putMetadata("updated", "true")
                        .build()
        ).await().indefinitely();

        GetDataSourceResponse getResponse = stub.getDataSource(
                GetDataSourceRequest.newBuilder()
                        .setDatasourceId(datasourceId)
                        .build()
        ).await().indefinitely();

        assertThat(getResponse.getDatasource().getName())
                .isEqualTo("E2E Lifecycle Test — Updated");
    }

    // =========================================================================
    // 7. API key rotation — old key invalidated, new key works
    // =========================================================================

    /**
     * Rotates the API key and verifies:
     * <ul>
     *   <li>The old key is immediately invalidated (uploads with old key are blocked).</li>
     *   <li>The new key is accepted (uploads are authorised again).</li>
     * </ul>
     *
     * <p>Key rotation is the recommended response to a potential credential leak.
     * After rotation the connector configuration must be updated with the new key.
     */
    @Test
    @Order(10)
    void step10_rotateApiKey_invalidatesOldKeyAndActivatesNew() {
        String oldKey = currentApiKey;

        RotateApiKeyResponse rotateResponse = stub.rotateApiKey(
                RotateApiKeyRequest.newBuilder()
                        .setDatasourceId(datasourceId)
                        .build()
        ).await().indefinitely();

        assertThat(rotateResponse.getSuccess()).as("rotation should succeed").isTrue();
        assertThat(rotateResponse.getNewApiKey())
                .as("new api_key should be returned once")
                .isNotBlank()
                .isNotEqualTo(oldKey);

        String newKey = rotateResponse.getNewApiKey();

        // Old key must be rejected immediately
        ValidateApiKeyResponse oldKeyResponse = stub.validateApiKey(
                ValidateApiKeyRequest.newBuilder()
                        .setDatasourceId(datasourceId)
                        .setApiKey(oldKey)
                        .build()
        ).await().indefinitely();

        assertThat(oldKeyResponse.getValid())
                .as("old API key should be invalid after rotation")
                .isFalse();

        // New key must be accepted
        ValidateApiKeyResponse newKeyResponse = stub.validateApiKey(
                ValidateApiKeyRequest.newBuilder()
                        .setDatasourceId(datasourceId)
                        .setApiKey(newKey)
                        .build()
        ).await().indefinitely();

        assertThat(newKeyResponse.getValid())
                .as("new API key should be valid after rotation")
                .isTrue();

        currentApiKey = newKey;
    }

    // =========================================================================
    // 8. Disable / re-enable (maintenance window)
    // =========================================================================

    /**
     * Disables the DataSource and confirms uploads are rejected, then re-enables
     * it and confirms uploads are authorised again.
     *
     * <p>Disabling a DataSource is used for maintenance windows, billing holds,
     * or security incidents.  No data is lost; the datasource can be re-enabled
     * at any time.
     */
    @Test
    @Order(11)
    void step11_disableAndReEnableDataSource_controlsUploadAccess() {
        // Disable
        SetDataSourceStatusResponse disableResponse = stub.setDataSourceStatus(
                SetDataSourceStatusRequest.newBuilder()
                        .setDatasourceId(datasourceId)
                        .setActive(false)
                        .setReason("it-maintenance-test")
                        .build()
        ).await().indefinitely();

        assertThat(disableResponse.getSuccess()).isTrue();

        // Upload should be rejected while disabled
        ValidateApiKeyResponse disabled = stub.validateApiKey(
                ValidateApiKeyRequest.newBuilder()
                        .setDatasourceId(datasourceId)
                        .setApiKey(currentApiKey)
                        .build()
        ).await().indefinitely();

        assertThat(disabled.getValid())
                .as("disabled datasource should reject API key validation")
                .isFalse();
        assertThat(disabled.getMessage())
                .as("rejection message should mention inactive state")
                .containsIgnoringCase("inactive");

        // Re-enable
        SetDataSourceStatusResponse enableResponse = stub.setDataSourceStatus(
                SetDataSourceStatusRequest.newBuilder()
                        .setDatasourceId(datasourceId)
                        .setActive(true)
                        .build()
        ).await().indefinitely();

        assertThat(enableResponse.getSuccess()).isTrue();

        // Upload should succeed again
        ValidateApiKeyResponse reenabled = stub.validateApiKey(
                ValidateApiKeyRequest.newBuilder()
                        .setDatasourceId(datasourceId)
                        .setApiKey(currentApiKey)
                        .build()
        ).await().indefinitely();

        assertThat(reenabled.getValid())
                .as("re-enabled datasource should accept API key validation")
                .isTrue();
    }

    // =========================================================================
    // 9. Soft delete
    // =========================================================================

    /**
     * Soft-deletes the DataSource and confirms it becomes inactive.
     *
     * <p>Soft deletion marks the datasource as inactive but retains the record for
     * audit purposes.  A hard delete can be performed via the
     * {@code CleanupTestDataSources} method (test environments only).
     */
    @Test
    @Order(12)
    void step12_deleteDataSource_marksInactive() {
        stub.deleteDataSource(
                DeleteDataSourceRequest.newBuilder()
                        .setDatasourceId(datasourceId)
                        .build()
        ).await().indefinitely();

        // Soft-deleted datasource should still be readable but inactive
        GetDataSourceResponse getResponse = stub.getDataSource(
                GetDataSourceRequest.newBuilder()
                        .setDatasourceId(datasourceId)
                        .build()
        ).await().indefinitely();

        assertThat(getResponse.getDatasource().getActive())
                .as("soft-deleted datasource should be inactive")
                .isFalse();

        // API key validation should be rejected for an inactive datasource
        ValidateApiKeyResponse validateDeleted = stub.validateApiKey(
                ValidateApiKeyRequest.newBuilder()
                        .setDatasourceId(datasourceId)
                        .setApiKey(currentApiKey)
                        .build()
        ).await().indefinitely();

        assertThat(validateDeleted.getValid())
                .as("deleted datasource should reject API key validation")
                .isFalse();
    }

}
