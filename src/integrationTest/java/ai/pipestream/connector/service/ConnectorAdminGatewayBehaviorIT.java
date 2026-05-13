package ai.pipestream.connector.service;

import ai.pipestream.connector.intake.v1.CreateConnectorTypeRequest;
import ai.pipestream.connector.intake.v1.CreateConnectorTypeResponse;
import ai.pipestream.connector.intake.v1.CreateDataSourceRequest;
import ai.pipestream.connector.intake.v1.CreateDataSourceResponse;
import ai.pipestream.connector.intake.v1.DataSourceAdminServiceGrpc;
import ai.pipestream.connector.intake.v1.DeleteConnectorTypeRequest;
import ai.pipestream.connector.intake.v1.DeleteDataSourceRequest;
import ai.pipestream.connector.intake.v1.GetDataSourceRequest;
import ai.pipestream.connector.intake.v1.ListDataSourcesRequest;
import ai.pipestream.connector.intake.v1.RotateApiKeyRequest;
import ai.pipestream.connector.intake.v1.SetDataSourceStatusRequest;
import ai.pipestream.connector.intake.v1.ValidateApiKeyRequest;
import ai.pipestream.connector.intake.v1.ValidateApiKeyResponse;
import ai.pipestream.connector.intake.v1.ConnectorRegistrationServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Packaged, black-box gateway tests for connector-admin.
 *
 * <p>These tests use standard grpc-java blocking stubs against the packaged
 * application. They intentionally avoid CDI, repositories, and in-process mocks:
 * PostgreSQL is real, and account-manager is represented only at the external
 * gRPC boundary by pipestream-wiremock-server.
 */
@QuarkusIntegrationTest
@QuarkusTestResource(ConnectorAdminIntegrationTestResource.class)
class ConnectorAdminGatewayBehaviorIT {

    private static final String ACTIVE_ACCOUNT_ID = "valid-account";
    private static final String INACTIVE_ACCOUNT_ID = "inactive-account";
    private static final String MISSING_ACCOUNT_ID = "nonexistent";

    @TestHTTPResource
    URL url;

    private ManagedChannel channel;
    private DataSourceAdminServiceGrpc.DataSourceAdminServiceBlockingStub admin;
    private ConnectorRegistrationServiceGrpc.ConnectorRegistrationServiceBlockingStub registration;

    @BeforeEach
    void setUp() {
        channel = ManagedChannelBuilder
            .forAddress(url.getHost(), url.getPort())
            .usePlaintext()
            .build();
        admin = DataSourceAdminServiceGrpc.newBlockingStub(channel);
        registration = ConnectorRegistrationServiceGrpc.newBlockingStub(channel);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        if (channel != null) {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void datasourceLifecycleProtectsTheCredentialBoundary() {
        String connectorId = createConnectorType("credential-boundary").getConnector().getConnectorId();
        String datasourceId = null;

        try {
            CreateDataSourceResponse created = admin.createDataSource(CreateDataSourceRequest.newBuilder()
                .setAccountId(ACTIVE_ACCOUNT_ID)
                .setConnectorId(connectorId)
                .setName("Credential Boundary Datasource")
                .setDriveName("credential-boundary-drive")
                .putMetadata("test", "credential-boundary")
                .build());

            assertThat(created.getSuccess()).isTrue();
            assertThat(created.getDatasource().getApiKey())
                .as("plaintext API key is returned only on create")
                .isNotBlank();
            datasourceId = created.getDatasource().getDatasourceId();
            String createdDatasourceId = datasourceId;
            String firstApiKey = created.getDatasource().getApiKey();

            assertThat(admin.getDataSource(GetDataSourceRequest.newBuilder()
                    .setDatasourceId(createdDatasourceId)
                    .build())
                .getDatasource()
                .getApiKey())
                .as("GetDataSource must not expose the plaintext API key")
                .isEmpty();

            assertThat(admin.listDataSources(ListDataSourcesRequest.newBuilder()
                    .setAccountId(ACTIVE_ACCOUNT_ID)
                    .setIncludeInactive(true)
                    .build())
                .getDatasourcesList())
                .as("ListDataSources must not expose plaintext API keys")
                .filteredOn(ds -> ds.getDatasourceId().equals(createdDatasourceId))
                .singleElement()
                .extracting(ds -> ds.getApiKey())
                .isEqualTo("");

            ValidateApiKeyResponse validFirstKey = admin.validateApiKey(ValidateApiKeyRequest.newBuilder()
                .setDatasourceId(createdDatasourceId)
                .setApiKey(firstApiKey)
                .build());
            assertThat(validFirstKey.getValid()).isTrue();
            assertThat(validFirstKey.getConfig().getDatasourceId()).isEqualTo(createdDatasourceId);
            assertThat(validFirstKey.getConfig().getDriveName()).isEqualTo("credential-boundary-drive");

            String rotatedKey = admin.rotateApiKey(RotateApiKeyRequest.newBuilder()
                    .setDatasourceId(createdDatasourceId)
                    .build())
                .getNewApiKey();
            assertThat(rotatedKey).isNotBlank().isNotEqualTo(firstApiKey);

            assertThat(admin.validateApiKey(ValidateApiKeyRequest.newBuilder()
                    .setDatasourceId(createdDatasourceId)
                    .setApiKey(firstApiKey)
                    .build())
                .getValid())
                .as("old key must be invalid immediately after rotation")
                .isFalse();
            assertThat(admin.validateApiKey(ValidateApiKeyRequest.newBuilder()
                    .setDatasourceId(createdDatasourceId)
                    .setApiKey(rotatedKey)
                    .build())
                .getValid())
                .as("new key must be valid after rotation")
                .isTrue();

            admin.setDataSourceStatus(SetDataSourceStatusRequest.newBuilder()
                .setDatasourceId(createdDatasourceId)
                .setActive(false)
                .setReason("gateway-behavior-test")
                .build());
            assertThat(admin.validateApiKey(ValidateApiKeyRequest.newBuilder()
                    .setDatasourceId(createdDatasourceId)
                    .setApiKey(rotatedKey)
                    .build())
                .getValid())
                .as("disabled datasources must not authenticate")
                .isFalse();
        } finally {
            softDelete(datasourceId);
            deleteConnectorBestEffort(connectorId);
        }
    }

    @Test
    void datasourceCreationRequiresAnActiveAccountAndExistingConnectorType() {
        String connectorId = createConnectorType("account-validation").getConnector().getConnectorId();

        try {
            StatusRuntimeException inactive = assertThrows(StatusRuntimeException.class, () ->
                admin.createDataSource(CreateDataSourceRequest.newBuilder()
                    .setAccountId(INACTIVE_ACCOUNT_ID)
                    .setConnectorId(connectorId)
                    .setName("Inactive Account Datasource")
                    .setDriveName("inactive-drive")
                    .build()));
            assertThat(inactive.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
            assertThat(inactive.getStatus().getDescription()).contains("inactive");

            StatusRuntimeException missing = assertThrows(StatusRuntimeException.class, () ->
                admin.createDataSource(CreateDataSourceRequest.newBuilder()
                    .setAccountId(MISSING_ACCOUNT_ID)
                    .setConnectorId(connectorId)
                    .setName("Missing Account Datasource")
                    .setDriveName("missing-drive")
                    .build()));
            assertThat(missing.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
            assertThat(missing.getStatus().getDescription()).contains("does not exist");

            StatusRuntimeException missingConnector = assertThrows(StatusRuntimeException.class, () ->
                admin.createDataSource(CreateDataSourceRequest.newBuilder()
                    .setAccountId(ACTIVE_ACCOUNT_ID)
                    .setConnectorId("00000000-0000-0000-0000-000000000000")
                    .setName("Missing Connector Datasource")
                    .setDriveName("missing-connector-drive")
                    .build()));
            assertThat(missingConnector.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND);
        } finally {
            deleteConnectorBestEffort(connectorId);
        }
    }

    @Test
    void connectorTypesCannotBeDeletedWhileReferencedByDatasources() {
        String connectorId = createConnectorType("referential-integrity").getConnector().getConnectorId();
        String datasourceId = null;

        try {
            CreateDataSourceResponse created = admin.createDataSource(CreateDataSourceRequest.newBuilder()
                .setAccountId(ACTIVE_ACCOUNT_ID)
                .setConnectorId(connectorId)
                .setName("Referential Integrity Datasource")
                .setDriveName("referential-integrity-drive")
                .build());
            datasourceId = created.getDatasource().getDatasourceId();

            StatusRuntimeException deleteRejected = assertThrows(StatusRuntimeException.class, () ->
                registration.deleteConnectorType(DeleteConnectorTypeRequest.newBuilder()
                    .setConnectorId(connectorId)
                    .build()));
            assertThat(deleteRejected.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);

            admin.deleteDataSource(DeleteDataSourceRequest.newBuilder()
                .setDatasourceId(datasourceId)
                .build());
            assertThat(admin.validateApiKey(ValidateApiKeyRequest.newBuilder()
                    .setDatasourceId(datasourceId)
                    .setApiKey(created.getDatasource().getApiKey())
                    .build())
                .getValid())
                .as("soft-deleted datasources remain present but cannot authenticate")
                .isFalse();
        } finally {
            softDelete(datasourceId);
            deleteConnectorBestEffort(connectorId);
        }
    }

    private CreateConnectorTypeResponse createConnectorType(String purpose) {
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        return registration.createConnectorType(CreateConnectorTypeRequest.newBuilder()
            .setConnectorType("it-" + purpose + "-" + suffix)
            .setName("Integration Test " + purpose)
            .setDescription("Created by connector-admin packaged integration tests")
            .setDisplayName("Integration Test " + purpose)
            .setOwner("connector-admin-tests")
            .setDocumentationUrl("https://docs.example.invalid/connector-admin/" + purpose)
            .addTags("integration-test")
            .addTags(purpose)
            .build());
    }

    private void softDelete(String datasourceId) {
        if (datasourceId == null || datasourceId.isBlank()) {
            return;
        }
        try {
            admin.deleteDataSource(DeleteDataSourceRequest.newBuilder()
                .setDatasourceId(datasourceId)
                .build());
        } catch (RuntimeException ignored) {
            // Best-effort cleanup. The assertion path above verifies observable behavior.
        }
    }

    private void deleteConnectorBestEffort(String connectorId) {
        if (connectorId == null || connectorId.isBlank()) {
            return;
        }
        try {
            registration.deleteConnectorType(DeleteConnectorTypeRequest.newBuilder()
                .setConnectorId(connectorId)
                .build());
        } catch (RuntimeException ignored) {
            // A connector with a soft-deleted datasource still has historical references.
        }
    }
}
