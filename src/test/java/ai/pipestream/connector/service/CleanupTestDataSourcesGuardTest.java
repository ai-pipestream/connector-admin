package ai.pipestream.connector.service;

import ai.pipestream.connector.intake.v1.CleanupTestDataSourcesRequest;
import ai.pipestream.connector.intake.v1.CleanupTestDataSourcesResponse;
import ai.pipestream.connector.intake.v1.MutinyDataSourceAdminServiceGrpc;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Prefix allowlist of {@code CleanupTestDataSources}: both ephemeral
 * naming conventions must pass ({@code test-} for integration tests,
 * {@code pipeline-crawl-} for the testing-sidecar's leaked-run sweep —
 * the sweep was previously rejected here, stranding datasource rows of
 * swept accounts), and anything else must stay rejected.
 */
@QuarkusTest
class CleanupTestDataSourcesGuardTest {

    @GrpcClient
    MutinyDataSourceAdminServiceGrpc.MutinyDataSourceAdminServiceStub dataSourceAdminService;

    private CleanupTestDataSourcesResponse cleanup(String accountId) {
        return dataSourceAdminService.cleanupTestDataSources(
                CleanupTestDataSourcesRequest.newBuilder().setAccountId(accountId).build())
                .await().indefinitely();
    }

    @Test
    void testPrefixIsAllowed() {
        CleanupTestDataSourcesResponse resp = cleanup("test-guard-probe-no-such-account");
        assertThat(resp.getSuccess()).isTrue();
        assertThat(resp.getDatasourcesDeleted()).isZero();
    }

    @Test
    void pipelineCrawlPrefixIsAllowed() {
        CleanupTestDataSourcesResponse resp = cleanup("pipeline-crawl-guard-probe-no-such-account");
        assertThat(resp.getSuccess()).isTrue();
        assertThat(resp.getDatasourcesDeleted()).isZero();
    }

    @Test
    void otherPrefixesAreRejected() {
        assertThatThrownBy(() -> cleanup("customer-prod-account"))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                        assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));
    }

    @Test
    void blankAccountIdIsRejected() {
        assertThatThrownBy(() -> cleanup(""))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                        assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));
    }
}
