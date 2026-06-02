package ai.pipestream.connector.service;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests the account validation stub mode used by in-JVM unit tests.
 */
@QuarkusTest
class AccountValidationServiceTest {

    @Inject
    AccountValidationService accountValidationService;

    @Test
    void validateAccountExistsAndActive_acceptsActiveStubAccount() {
        assertDoesNotThrow(() -> accountValidationService.validateAccountExistsAndActive("default-account"));
    }

    @Test
    void validateAccountExistsAndActive_rejectsInactiveAccount() {
        StatusRuntimeException ex = assertThrows(StatusRuntimeException.class,
            () -> accountValidationService.validateAccountExistsAndActive("inactive-account"));

        assertThat(ex.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
        assertThat(ex.getStatus().getDescription()).contains("inactive");
    }

    @Test
    void validateAccountExistsAndActive_rejectsMissingAccount() {
        StatusRuntimeException ex = assertThrows(StatusRuntimeException.class,
            () -> accountValidationService.validateAccountExistsAndActive("nonexistent"));

        assertThat(ex.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
        assertThat(ex.getStatus().getDescription()).contains("does not exist");
    }

    @Test
    void validateAccountExists_acceptsInactiveAccount() {
        assertDoesNotThrow(() -> accountValidationService.validateAccountExists("inactive-account"));
    }

    @Test
    void validateAccountExists_rejectsMissingAccount() {
        StatusRuntimeException ex = assertThrows(StatusRuntimeException.class,
            () -> accountValidationService.validateAccountExists("nonexistent"));

        assertThat(ex.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
    }
}
