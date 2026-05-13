package ai.pipestream.connector.service;

import ai.pipestream.connector.entity.DataSource;
import ai.pipestream.repository.account.v1.AccountEvent;
import io.quarkus.narayana.jta.QuarkusTransaction;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

@QuarkusTest
class AccountEventListenerTest {

    private static final String S3_CONNECTOR_ID = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11";
    private static final String ACCOUNT_ID = "account-event-listener-test";
    private static final String DATASOURCE_ID = UUID.nameUUIDFromBytes(
        (ACCOUNT_ID + ":" + S3_CONNECTOR_ID).getBytes()).toString();

    @Inject
    AccountEventListener listener;

    @BeforeEach
    void setUp() {
        QuarkusTransaction.requiringNew().run(() ->
            DataSource.delete("accountId", ACCOUNT_ID)
        );
    }

    @Test
    void inactivatedEventDisablesActiveDatasources() {
        createDatasource(true, null);

        handleWithoutRequestContext(AccountEvent.newBuilder()
            .setEventId("test-inactivated")
            .setAccountId(ACCOUNT_ID)
            .setInactivated(AccountEvent.Inactivated.newBuilder().setReason("cleanup"))
            .build());

        DataSource datasource = findDatasource();
        assertThat(datasource.active).isFalse();
        assertThat(datasource.statusReason).isEqualTo("account_inactive");
    }

    @Test
    void reactivatedEventReEnablesDatasourcesDisabledByAccountInactivation() {
        createDatasource(false, "account_inactive");

        handleWithoutRequestContext(AccountEvent.newBuilder()
            .setEventId("test-reactivated")
            .setAccountId(ACCOUNT_ID)
            .setReactivated(AccountEvent.Reactivated.newBuilder().setReason("cleanup"))
            .build());

        DataSource datasource = findDatasource();
        assertThat(datasource.active).isTrue();
        assertThat(datasource.statusReason).isNull();
    }

    private void createDatasource(boolean active, String statusReason) {
        QuarkusTransaction.requiringNew().run(() -> {
            DataSource datasource = new DataSource(
                DATASOURCE_ID,
                ACCOUNT_ID,
                S3_CONNECTOR_ID,
                "Account Event Listener Test",
                "$argon2id$v=19$m=65536,t=3,p=4$test$test",
                "account-event-test-drive");
            datasource.active = active;
            datasource.statusReason = statusReason;
            datasource.persist();
        });
    }

    private DataSource findDatasource() {
        return QuarkusTransaction.requiringNew().call(() -> DataSource.findById(DATASOURCE_ID));
    }

    private void handleWithoutRequestContext(AccountEvent event) {
        CompletableFuture.runAsync(() -> listener.handleAccountEvent(event)).join();
    }
}
