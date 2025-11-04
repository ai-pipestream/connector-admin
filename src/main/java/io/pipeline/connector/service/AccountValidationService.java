package io.pipeline.connector.service;

import io.pipeline.dynamic.grpc.client.DynamicGrpcClientFactory;
import io.pipeline.repository.account.GetAccountRequest;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

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
    private static final String ACCOUNT_SERVICE_NAME = "account-manager";

    @Inject
    DynamicGrpcClientFactory grpcClientFactory;

    /**
     * Validates that an account exists and is active.
     * <p>
     * Invokes account-manager's `GetAccount` RPC via dynamic-grpc service discovery.
     *
     * Reactive semantics:
     * - Returns a cold `Uni` that performs exactly one remote call and completes empty on success.
     * - Remote failures are propagated as `StatusRuntimeException` unless mapped as described below.
     *
     * Side effects:
     * - Remote gRPC call to the account-manager service; writes logs for observability.
     *
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

        return grpcClientFactory.getAccountServiceClient(ACCOUNT_SERVICE_NAME)
            .flatMap(stub -> stub.getAccount(
                GetAccountRequest.newBuilder()
                    .setAccountId(accountId)
                    .build()
            ))
            .flatMap(account -> {
                if (!account.getActive()) {
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
            .onFailure(io.grpc.StatusRuntimeException.class)
            .transform(throwable -> {
                // Cast to StatusRuntimeException (we know it is because of onFailure filter)
                io.grpc.StatusRuntimeException sre = (io.grpc.StatusRuntimeException) throwable;
                // Check if it's NOT_FOUND from account service
                if (sre.getStatus().getCode() == io.grpc.Status.Code.NOT_FOUND) {
                    LOG.warnf("Account not found: %s", accountId);
                    return io.grpc.Status.INVALID_ARGUMENT
                        .withDescription("Account does not exist: " + accountId)
                        .asRuntimeException();
                }
                // Propagate other gRPC errors (UNAVAILABLE, etc.)
                LOG.errorf(sre, "Failed to validate account %s", accountId);
                return sre;
            });
    }

    /**
     * Validates that an account exists (regardless of active status).
     * <p>
     * Used when existence is required but active/inactive state does not matter.
     *
     * Reactive semantics:
     * - Returns a cold `Uni` that performs exactly one `GetAccount` remote call and completes empty on success.
     * - Remote failures are propagated as `StatusRuntimeException` unless mapped as described below.
     *
     * Side effects:
     * - Remote gRPC call to the account-manager service; writes logs for observability.
     *
     * Error mapping:
     * - NOT_FOUND from account-manager → mapped to INVALID_ARGUMENT ("Account does not exist: <id>").
     * - Other gRPC codes (e.g., UNAVAILABLE, DEADLINE_EXCEEDED) are propagated unchanged.
     *
     * @param accountId Account identifier to validate; must be non-null and non-empty.
     * @return `Uni` that completes with `void` if the account exists; fails otherwise per mapping above.
     */
    public Uni<Void> validateAccountExists(String accountId) {
        LOG.debugf("Validating account exists: %s", accountId);

        return grpcClientFactory.getAccountServiceClient(ACCOUNT_SERVICE_NAME)
            .flatMap(stub -> stub.getAccount(
                GetAccountRequest.newBuilder()
                    .setAccountId(accountId)
                    .build()
            ))
            .replaceWithVoid()
            .onFailure(io.grpc.StatusRuntimeException.class)
            .transform(throwable -> {
                io.grpc.StatusRuntimeException sre = (io.grpc.StatusRuntimeException) throwable;
                if (sre.getStatus().getCode() == io.grpc.Status.Code.NOT_FOUND) {
                    LOG.warnf("Account not found: %s", accountId);
                    return io.grpc.Status.INVALID_ARGUMENT
                        .withDescription("Account does not exist: " + accountId)
                        .asRuntimeException();
                }
                LOG.errorf(sre, "Failed to validate account %s", accountId);
                return sre;
            });
    }
}
