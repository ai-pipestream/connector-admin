package ai.pipestream.connector.service;

import ai.pipestream.repository.account.v1.AccountServiceGrpc;
import ai.pipestream.repository.account.v1.GetAccountRequest;
import ai.pipestream.repository.account.v1.GetAccountResponse;
import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

/**
 * Validates accounts by calling account-manager over standard blocking gRPC.
 */
@ApplicationScoped
public class AccountValidationService {

    private static final Logger LOG = Logger.getLogger(AccountValidationService.class);

    @GrpcClient("account-manager")
    Channel accountChannel;

    private AccountServiceGrpc.AccountServiceBlockingStub accountStub;

    @ConfigProperty(name = "connector.admin.account.validation.stub", defaultValue = "false")
    boolean stubValidation;

    @PostConstruct
    void init() {
        this.accountStub = AccountServiceGrpc.newBlockingStub(accountChannel);
    }

    public void validateAccountExistsAndActive(String accountId) {
        if (stubValidation) {
            validateStubAccount(accountId, true);
            return;
        }

        GetAccountResponse response = getAccountOrMapNotFound(accountId);
        if (!response.getAccount().getActive()) {
            throw Status.INVALID_ARGUMENT
                .withDescription("Account is inactive: " + accountId)
                .asRuntimeException();
        }
        LOG.debugf("Account %s validated successfully", accountId);
    }

    public void validateAccountExists(String accountId) {
        if (stubValidation) {
            validateStubAccount(accountId, false);
            return;
        }
        getAccountOrMapNotFound(accountId);
    }

    private GetAccountResponse getAccountOrMapNotFound(String accountId) {
        try {
            return accountStub.getAccount(GetAccountRequest.newBuilder().setAccountId(accountId).build());
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
                throw Status.INVALID_ARGUMENT
                    .withDescription("Account does not exist: " + accountId)
                    .asRuntimeException();
            }
            throw e;
        }
    }

    private void validateStubAccount(String accountId, boolean requireActive) {
        if (accountId == null || accountId.isBlank()) {
            throw Status.INVALID_ARGUMENT.withDescription("Account ID is required").asRuntimeException();
        }
        if ("missing".equals(accountId) || "nonexistent".equals(accountId)) {
            throw Status.INVALID_ARGUMENT.withDescription("Account does not exist: " + accountId).asRuntimeException();
        }
        if (requireActive && "inactive-account".equals(accountId)) {
            throw Status.INVALID_ARGUMENT.withDescription("Account is inactive: " + accountId).asRuntimeException();
        }
    }
}
