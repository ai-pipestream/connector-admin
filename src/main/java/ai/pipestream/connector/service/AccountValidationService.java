package ai.pipestream.connector.service;

import ai.pipestream.repository.account.v1.AccountServiceGrpc;
import ai.pipestream.repository.account.v1.GetAccountRequest;
import ai.pipestream.repository.account.v1.GetAccountResponse;
import io.grpc.Channel;
import io.grpc.stub.StreamObserver;
import io.quarkus.grpc.GrpcClient;
import io.smallrye.mutiny.Uni;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.concurrent.CompletableFuture;

/**
 * Validates accounts by calling account-manager over gRPC.
 * <p>
 * Acts as the explicit integration boundary between connector-service and the
 * account-manager microservice, avoiding direct database coupling.
 * <p>
 * Reactive semantics:
 * <ul>
 *   <li>All methods return a cold {@code Uni} that performs a single remote gRPC call.</li>
 *   <li>Execution happens off the event loop as part of the gRPC client I/O; no blocking in user code.</li>
 *   <li>Failures from the remote service are propagated as {@code StatusRuntimeException} unless mapped as documented.</li>
 * </ul>
 *
 * Side effects:
 * <ul>
 *   <li>Remote call to account-manager using dynamic-grpc with Stork service discovery.</li>
 *   <li>Writes debug/warn/error logs for observability.</li>
 * </ul>
 */
@ApplicationScoped
public class AccountValidationService {

    private static final Logger LOG = Logger.getLogger(AccountValidationService.class);

    /**
     * Stock Quarkus gRPC channel for {@code account-manager}. Built once at
     * app start and lives the JVM lifetime — no in-house
     * DynamicGrpcClientFactory cache, no TTL, no eviction. Quarkus's
     * {@link GrpcClient} only permits Mutiny stubs, blocking stubs, or a
     * raw {@link Channel}; the StreamObserver-based async stub
     * ({@code AccountServiceStub}) is built once from the channel in
     * {@link #init()}. Async stub gives non-blocking call sites, so the
     * gRPC handler thread is not held during the round-trip — much
     * higher throughput than the blocking stub.
     */
    @GrpcClient("account-manager")
    Channel accountChannel;

    private AccountServiceGrpc.AccountServiceStub accountStub;

    @PostConstruct
    void init() {
        this.accountStub = AccountServiceGrpc.newStub(accountChannel);
    }

    // Test-only stub: enabled in %test profile to avoid external dependency in unit tests
    @ConfigProperty(name = "connector.admin.account.validation.stub", defaultValue = "false")
    boolean stubValidation;

    /**
     * Validates that an account exists and is active.
     * <p>
     * Invokes account-manager's `GetAccount` RPC via dynamic-grpc service discovery.
     * <p>
     * Reactive semantics:
     * - Returns a cold `Uni` that performs exactly one remote call and completes empty on success.
     * - Remote failures are propagated as `StatusRuntimeException` unless mapped as described below.
     * <p>
     * Side effects:
     * - Remote gRPC call to the account-manager service; writes logs for observability.
     * <p>
     * Error mapping:
     * - NOT_FOUND from account-manager → mapped to INVALID_ARGUMENT ("Account does not exist: <id>").
     * - Account present but inactive → mapped to INVALID_ARGUMENT ("Account is inactive: <id>").
     * - Other gRPC codes (e.g., UNAVAILABLE, DEADLINE_EXCEEDED) are propagated unchanged.
     *
     * @param accountId Account identifier to validate; must be non-null and non-empty.
     * @return `Uni` that completes with `void` if the account exists and is active; fails otherwise per mapping above.
     */
    public Uni<Void> validateAccountExistsAndActive(String accountId) {
        LOG.debugf("Validating account exists and is active: %s", accountId);
        LOG.debugf("Stub validation mode: %s", stubValidation);

        if (stubValidation) {
            LOG.infof("Using STUB MODE for account validation: %s", accountId);
            // Test-mode behavior: emulate typical scenarios used in tests
            if (accountId == null || accountId.isBlank()) {
                return Uni.createFrom().failure(
                    io.grpc.Status.INVALID_ARGUMENT.withDescription("Account ID is required").asRuntimeException()
                );
            }
            if ("nonexistent".equals(accountId)) {
                return Uni.createFrom().failure(
                    io.grpc.Status.INVALID_ARGUMENT.withDescription("Account does not exist: " + accountId).asRuntimeException()
                );
            }
            if ("inactive-account".equals(accountId)) {
                return Uni.createFrom().failure(
                    io.grpc.Status.INVALID_ARGUMENT.withDescription("Account is inactive: " + accountId).asRuntimeException()
                );
            }
            // All other accounts are considered active in test mode
            return Uni.createFrom().voidItem();
        }

        return getAccount(accountId)
            .flatMap((GetAccountResponse resp) -> {
                if (!resp.getAccount().getActive()) {
                    LOG.warnf("Account %s exists but is inactive", accountId);
                    return Uni.createFrom().failure(
                        io.grpc.Status.INVALID_ARGUMENT
                            .withDescription("Account is inactive: " + accountId)
                            .asRuntimeException()
                    );
                }
                LOG.debugf("Account %s validated successfully", accountId);
                return Uni.createFrom().voidItem();
            })
            .onFailure(io.grpc.StatusRuntimeException.class).transform(throwable -> {
                io.grpc.StatusRuntimeException sre = (io.grpc.StatusRuntimeException) throwable;
                if (sre.getStatus().getCode() == io.grpc.Status.Code.NOT_FOUND) {
                    LOG.warnf("Account not found: %s", accountId);
                    return io.grpc.Status.INVALID_ARGUMENT
                            .withDescription("Account does not exist: " + accountId)
                            .asRuntimeException();
                }
                // Propagate other gRPC errors (UNAVAILABLE, etc.)
                LOG.errorf(throwable, "Failed to validate account %s", accountId);
                return throwable;
            });
    }

    /**
     * Validates that an account exists (regardless of active status).
     * <p>
     * Used when existence is required but active/inactive state does not matter.
     * <p>
     * Reactive semantics:
     * - Returns a cold `Uni` that performs exactly one `GetAccount` remote call and completes empty on success.
     * - Remote failures are propagated as `StatusRuntimeException` unless mapped as described below.
     * <p>
     * Side effects:
     * - Remote gRPC call to the account-manager service; writes logs for observability.
     * <p>
     * Error mapping:
     * - NOT_FOUND from account-manager → mapped to INVALID_ARGUMENT ("Account does not exist: <id>").
     * - Other gRPC codes (e.g., UNAVAILABLE, DEADLINE_EXCEEDED) are propagated unchanged.
     *
     * @param accountId Account identifier to validate; must be non-null and non-empty.
     * @return `Uni` that completes with `void` if the account exists; fails otherwise per mapping above.
     */
    public Uni<Void> validateAccountExists(String accountId) {
        LOG.debugf("Validating account exists: %s", accountId);

        if (stubValidation) {
            if (accountId == null || accountId.isBlank()) {
                return Uni.createFrom().failure(
                    io.grpc.Status.INVALID_ARGUMENT.withDescription("Account ID is required").asRuntimeException()
                );
            }
            if ("missing".equals(accountId) || "nonexistent".equals(accountId)) {
                return Uni.createFrom().failure(
                    io.grpc.Status.INVALID_ARGUMENT.withDescription("Account does not exist: " + accountId).asRuntimeException()
                );
            }
            // All other accounts are considered to exist in test mode
            return Uni.createFrom().voidItem();
        }

        return getAccount(accountId)
            .replaceWithVoid()
            .onFailure(io.grpc.StatusRuntimeException.class).transform(throwable -> {
                io.grpc.StatusRuntimeException sre = (io.grpc.StatusRuntimeException) throwable;
                if (sre.getStatus().getCode() == io.grpc.Status.Code.NOT_FOUND) {
                    LOG.warnf("Account not found: %s", accountId);
                    return io.grpc.Status.INVALID_ARGUMENT
                            .withDescription("Account does not exist: " + accountId)
                            .asRuntimeException();
                }
                LOG.errorf(throwable, "Failed to validate account %s", accountId);
                return throwable;
            });
    }

    /**
     * Bridges the StreamObserver-based async stub to a {@link Uni}. The
     * gRPC call's {@code io.grpc.Context} is captured synchronously inside
     * this method when {@code accountStub.getAccount(req, observer)} is
     * invoked — no async stub-resolution gap, no Mutiny operator that
     * re-emits on a different thread. Non-blocking: the calling thread
     * returns immediately; the StreamObserver callbacks fire on gRPC's
     * internal threads, complete the future, and the upstream Mutiny
     * chain resumes from there.
     */
    private Uni<GetAccountResponse> getAccount(String accountId) {
        GetAccountRequest request = GetAccountRequest.newBuilder()
            .setAccountId(accountId)
            .build();
        CompletableFuture<GetAccountResponse> future = new CompletableFuture<>();
        accountStub.getAccount(request, new StreamObserver<>() {
            @Override
            public void onNext(GetAccountResponse value) {
                future.complete(value);
            }

            @Override
            public void onError(Throwable t) {
                future.completeExceptionally(t);
            }

            @Override
            public void onCompleted() {
                // no-op for unary
            }
        });
        return Uni.createFrom().completionStage(future);
    }
}
