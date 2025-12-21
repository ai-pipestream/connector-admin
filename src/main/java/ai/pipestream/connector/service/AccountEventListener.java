package ai.pipestream.connector.service;

import ai.pipestream.repository.account.v1.AccountEvent;
import ai.pipestream.connector.entity.DataSource;
import ai.pipestream.connector.repository.DataSourceRepository;
import io.smallrye.reactive.messaging.annotations.Blocking;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.logging.Logger;

import java.util.List;

/**
 * Consumes account lifecycle events and synchronizes datasource status.
 * <p>
 * Listens for account inactivation/reactivation events and automatically
 * disables or re-enables datasources for those accounts to preserve
 * authorization invariants across services.
 * <p>
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
 * <li>Writes to the datasources table via {@link DataSourceRepository} to
 * enable/disable datasources.</li>
 * <li>Emits INFO/WARN logs for auditing.</li>
 * </ul>
 */
@ApplicationScoped
public class AccountEventListener {

    private static final Logger LOG = Logger.getLogger(AccountEventListener.class);

    @Inject
    DataSourceRepository dataSourceRepository;

    /**
     * Handles an account lifecycle event and reconciles datasource status
     * accordingly.
     * <p>
     * - On inactivation: disables all datasources for the account with
     * status_reason="account_inactive".
     * - On reactivation: re-enables datasources that were disabled for
     * status_reason="account_inactive".
     * <p>
     * Reactive semantics:
     * - Consumed from `account-events` channel; {@code @Blocking} ensures work runs
     * on a worker thread.
     * - Per-partition ordering is preserved by the messaging provider; do not rely
     * on global ordering.
     * - Handler is idempotent to tolerate at-least-once delivery.
     * <p>
     * Side effects:
     * - Writes to the datasources table via {@link DataSourceRepository}.
     * - Emits INFO/WARN logs for auditing and diagnostics.
     *
     * @param event Account event payload carrying the operation and account
     *              identifier.
     */
    @Incoming("account-events")
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
     * Disables all datasources for the account and sets
     * status_reason="account_inactive".
     */
    private void handleAccountInactivated(String accountId, String reason) {
        LOG.infof("Account inactivated: accountId=%s, reason=%s", accountId, reason);

        // Find all datasources for this account
        List<DataSource> datasources = dataSourceRepository.listByAccount(accountId, true, 0, 0);

        if (datasources.isEmpty()) {
            LOG.debugf("No datasources found for account: %s", accountId);
            return;
        }

        int disabledCount = 0;
        for (DataSource datasource : datasources) {
            // Only disable if currently active
            if (datasource.active) {
                dataSourceRepository.setDataSourceStatus(datasource.datasourceId, false, "account_inactive");
                disabledCount++;
                LOG.infof("Disabled datasource: datasourceId=%s, accountId=%s, reason=account_inactive",
                        datasource.datasourceId, accountId);
            } else {
                LOG.debugf("DataSource already inactive: datasourceId=%s", datasource.datasourceId);
            }
        }

        LOG.infof("Account inactivation complete: accountId=%s, datasourcesDisabled=%d", accountId, disabledCount);
    }

    /**
     * Handle account reactivation.
     * <p>
     * Re-enables datasources that were disabled due to account inactivation.
     * Only re-enables datasources with status_reason="account_inactive".
     */
    private void handleAccountReactivated(String accountId, String reason) {
        LOG.infof("Account reactivated: accountId=%s, reason=%s", accountId, reason);

        // Find all datasources for this account (including inactive)
        List<DataSource> datasources = dataSourceRepository.listByAccount(accountId, true, 0, 0);

        if (datasources.isEmpty()) {
            LOG.debugf("No datasources found for account: %s", accountId);
            return;
        }

        int reactivatedCount = 0;
        for (DataSource datasource : datasources) {
            // Only re-enable if disabled due to account inactivation
            if (!datasource.active && "account_inactive".equals(datasource.statusReason)) {
                dataSourceRepository.setDataSourceStatus(datasource.datasourceId, true, null);
                reactivatedCount++;
                LOG.infof("Re-enabled datasource: datasourceId=%s, accountId=%s",
                        datasource.datasourceId, accountId);
            } else {
                LOG.debugf("DataSource not eligible for re-enablement: datasourceId=%s, active=%s, statusReason=%s",
                        datasource.datasourceId, datasource.active, datasource.statusReason);
            }
        }

        LOG.infof("Account reactivation complete: accountId=%s, datasourcesReactivated=%d", accountId, reactivatedCount);
    }
}
