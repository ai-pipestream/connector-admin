package ai.pipestream.connector.service;

import ai.pipestream.repository.v1.account.AccountEvent;
import ai.pipestream.connector.entity.ConnectorAccount;
import ai.pipestream.connector.repository.ConnectorRepository;
import io.smallrye.reactive.messaging.annotations.Blocking;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import ai.pipestream.apicurio.registry.protobuf.ProtobufIncoming;
import org.jboss.logging.Logger;

import java.util.List;

/**
 * Consumes account lifecycle events and synchronizes connector status.
 * <p>
 * Listens for account inactivation/reactivation events and automatically
 * disables or re-enables connectors linked to those accounts to preserve
 * authorization invariants across services.
 *
 * Reactive semantics:
 * <ul>
 * <li>Inbound channel `account-events` is consumed via MicroProfile Reactive
 * Messaging.</li>
 * <li>{@code @Blocking} ensures processing happens on a worker thread; the
 * broker thread is not blocked.</li>
 * <li>Per-partition ordering is preserved by the messaging provider;
 * applications should not assume global ordering.</li>
 * <li>Handlers are designed to be idempotent to tolerate redelivery
 * (at-least-once semantics).</li>
 * </ul>
 *
 * Side effects:
 * <ul>
 * <li>Writes to the connectors table via {@link ConnectorRepository} to
 * enable/disable connectors.</li>
 * <li>Emits INFO/WARN logs for auditing.</li>
 * </ul>
 */
@ApplicationScoped
public class AccountEventListener {

    private static final Logger LOG = Logger.getLogger(AccountEventListener.class);

    @Inject
    ConnectorRepository connectorRepository;

    /**
     * Handles an account lifecycle event and reconciles connector status
     * accordingly.
     * <p>
     * - On inactivation: disables all connectors linked to the account with
     * status_reason="account_inactive".
     * - On reactivation: re-enables connectors that were disabled for
     * status_reason="account_inactive".
     *
     * Reactive semantics:
     * - Consumed from `account-events` channel; {@code @Blocking} ensures work runs
     * on a worker thread.
     * - Per-partition ordering is preserved by the messaging provider; do not rely
     * on global ordering.
     * - Handler is idempotent to tolerate at-least-once delivery.
     *
     * Side effects:
     * - Writes to the connectors table via {@link ConnectorRepository}.
     * - Emits INFO/WARN logs for auditing and diagnostics.
     *
     * @param event Account event payload carrying the operation and account
     *              identifier.
     */
    @ProtobufIncoming("account-events")
    @Blocking
    public void handleAccountEvent(AccountEvent event) {
        String accountId = event.getAccountId();

        LOG.infof("Received account event: eventId=%s, accountId=%s, operation=%s",
                event.getEventId(), accountId, event.getOperationCase());

        try {
            switch (event.getOperationCase()) {
                case INACTIVATED:
                    handleAccountInactivated(accountId, event.getInactivated().getReason());
                    break;
                case REACTIVATED:
                    handleAccountReactivated(accountId, event.getReactivated().getReason());
                    break;
                case CREATED:
                case UPDATED:
                    // No action needed for created/updated events
                    LOG.debugf("Ignoring account %s event: %s", event.getOperationCase(), accountId);
                    break;
                case OPERATION_NOT_SET:
                    LOG.warnf("Received account event with no operation set: eventId=%s", event.getEventId());
                    break;
            }
        } catch (Exception e) {
            LOG.errorf(e, "Error processing account event: eventId=%s, accountId=%s",
                    event.getEventId(), accountId);
            // Don't throw - we want to continue processing other events
        }
    }

    /**
     * Handle account inactivation.
     * <p>
     * Disables all connectors for the account and sets
     * status_reason="account_inactive".
     */
    private void handleAccountInactivated(String accountId, String reason) {
        LOG.infof("Account inactivated: accountId=%s, reason=%s", accountId, reason);

        // Find all connectors for this account
        List<ConnectorAccount> connectorAccounts = connectorRepository.findByAccountId(accountId);

        if (connectorAccounts.isEmpty()) {
            LOG.debugf("No connectors found for account: %s", accountId);
            return;
        }

        int disabledCount = 0;
        for (ConnectorAccount connectorAccount : connectorAccounts) {
            String connectorId = connectorAccount.connectorId;

            // Only disable if currently active
            if (connectorAccount.connector.active) {
                connectorRepository.disableConnector(connectorId, "account_inactive");
                disabledCount++;
                LOG.infof("Disabled connector: connectorId=%s, accountId=%s, reason=account_inactive",
                        connectorId, accountId);
            } else {
                LOG.debugf("Connector already inactive: connectorId=%s", connectorId);
            }
        }

        LOG.infof("Account inactivation complete: accountId=%s, connectorsDisabled=%d", accountId, disabledCount);
    }

    /**
     * Handle account reactivation.
     * <p>
     * Re-enables connectors that were disabled due to account inactivation.
     * Only re-enables connectors with status_reason="account_inactive".
     */
    private void handleAccountReactivated(String accountId, String reason) {
        LOG.infof("Account reactivated: accountId=%s, reason=%s", accountId, reason);

        // Find all connectors for this account
        List<ConnectorAccount> connectorAccounts = connectorRepository.findByAccountId(accountId);

        if (connectorAccounts.isEmpty()) {
            LOG.debugf("No connectors found for account: %s", accountId);
            return;
        }

        int reactivatedCount = 0;
        for (ConnectorAccount connectorAccount : connectorAccounts) {
            String connectorId = connectorAccount.connectorId;

            // Only re-enable if disabled due to account inactivation
            if (!connectorAccount.connector.active
                    && "account_inactive".equals(connectorAccount.connector.statusReason)) {
                connectorRepository.enableConnector(connectorId);
                reactivatedCount++;
                LOG.infof("Re-enabled connector: connectorId=%s, accountId=%s",
                        connectorId, accountId);
            } else {
                LOG.debugf("Connector not eligible for re-enablement: connectorId=%s, active=%s, statusReason=%s",
                        connectorId, connectorAccount.connector.active, connectorAccount.connector.statusReason);
            }
        }

        LOG.infof("Account reactivation complete: accountId=%s, connectorsReactivated=%d", accountId, reactivatedCount);
    }
}
