package ai.pipestream.connector.service;

import ai.pipestream.connector.entity.DataSource;
import ai.pipestream.connector.intake.v1.CreateDataSourceRequest;
import ai.pipestream.connector.intake.v1.MutinyDataSourceAdminServiceGrpc;
import ai.pipestream.test.support.ConnectorAdminWireMockTestResource;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.hibernate.reactive.panache.Panache;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.vertx.RunOnVertxContext;
import io.quarkus.test.vertx.UniAsserter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for {@code "No current Vertx context found"} on CreateDataSource.
 *
 * <p>The handler chain in {@code DataSourceAdminServiceImpl.createDataSource}
 * makes an outbound gRPC call to {@code account-manager} and then uses
 * Hibernate Reactive ({@code Panache.withTransaction}) to write the new
 * datasource row. The real Mutiny gRPC client completes its {@link
 * io.smallrye.mutiny.Uni} on a {@code grpc-default-executor} thread (NOT a
 * Vert.x event loop). When the downstream operator then invokes
 * {@code Panache.withTransaction(...)}, Hibernate Reactive's
 * {@code SessionOperations.vertxContext()} runs synchronously, finds
 * {@code Vertx.currentContext() == null}, and throws
 * {@code IllegalStateException: No current Vertx context found}.
 *
 * <p>Other tests in this module run with
 * {@code connector.admin.account.validation.stub=true} (the {@code %test}
 * profile default in {@code application.properties}). That stub returns
 * {@code Uni.createFrom().voidItem()} synchronously on the calling thread, so
 * the chain stays on the Vert.x event loop and the bug never surfaces. This
 * test deliberately disables the stub via {@link Profile} and routes the real
 * Mutiny client at a {@link ConnectorAdminWireMockTestResource}-managed
 * WireMock container, reproducing the production threading shape.
 *
 * <p>Without the {@code emitOn(callerCtx::runOnContext)} hop in
 * {@code createDataSource}, this test fails with
 * {@code IllegalStateException}. Do not "fix" the test by re-enabling the
 * stub — that defeats the regression check.
 */
@QuarkusTest
@QuarkusTestResource(ConnectorAdminWireMockTestResource.class)
@TestProfile(DataSourceAdminOffEventLoopAsyncTest.Profile.class)
public class DataSourceAdminOffEventLoopAsyncTest {

    private static final String TEST_CONNECTOR_ID = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"; // pre-seeded S3 connector
    /** Configured by {@link ConnectorAdminWireMockTestResource} as an active account. */
    private static final String VALID_ACCOUNT_ID = "valid-account";

    @GrpcClient
    MutinyDataSourceAdminServiceGrpc.MutinyDataSourceAdminServiceStub dataSourceAdminService;

    @BeforeEach
    @RunOnVertxContext
    void cleanup(UniAsserter asserter) {
        asserter.execute(() -> Panache.withTransaction(() -> DataSource.deleteAll()));
    }

    @Test
    void createDataSource_succeeds_whenAccountValidationCompletesOffEventLoop() {
        CreateDataSourceRequest request = CreateDataSourceRequest.newBuilder()
            .setAccountId(VALID_ACCOUNT_ID)
            .setConnectorId(TEST_CONNECTOR_ID)
            .setName("Off-Event-Loop Regression DataSource " + System.nanoTime())
            .setDriveName("test-drive")
            .build();

        var response = dataSourceAdminService.createDataSource(request)
            .await().indefinitely();

        assertThat(response.getSuccess())
            .as("createDataSource must succeed when account validation goes through the real Mutiny gRPC "
                + "client (callback on grpc-default-executor, NOT a Vert.x event loop). If this fails with "
                + "'No current Vertx context found' the emitOn(callerCtx::runOnContext) hop in "
                + "DataSourceAdminServiceImpl.createDataSource has regressed.")
            .isTrue();
        assertThat(response.getDatasource().getDatasourceId())
            .as("a successful response must include a generated datasource id")
            .isNotEmpty();
        assertThat(response.getDatasource().getApiKey())
            .as("a freshly created datasource must include the one-time API key in the response")
            .isNotEmpty();
    }

    /**
     * Disables the {@code AccountValidationService} stub so the real Mutiny
     * gRPC client is exercised against the WireMock container. Without this
     * the {@code %test} profile in {@code application.properties} would force
     * stub mode and hide the threading bug.
     */
    public static class Profile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("connector.admin.account.validation.stub", "false");
        }
    }
}
