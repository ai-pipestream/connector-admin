package ai.pipestream.connector.service;

import ai.pipestream.repository.account.v1.AccountEvent;
import ai.pipestream.connector.entity.DataSource;
import ai.pipestream.connector.repository.DataSourceRepository;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.logging.Logger;

import java.util.List;
import java.util.concurrent.CompletionStage;

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
     * - Consumed from `account-events` channel; returns CompletionStage for async processing.
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
     * @return CompletionStage that completes when processing is done
     */
    @Incoming("account-events")
    public CompletionStage<Void> handleAccountEvent(AccountEvent event) {
        String accountId = event.getAccountId();

        LOG.infof("Received account event: eventId=%s, accountId=%s, operation=%s",
                event.getEventId(), accountId, event.getOperationCase());

        Uni<Void> processingUni = switch (event.getOperationCase()) {
            case INACTIVATED -> handleAccountInactivated(accountId, event.getInactivated().getReason());
            case REACTIVATED -> handleAccountReactivated(accountId, event.getReactivated().getReason());
            case CREATED, UPDATED -> {
                // No action needed for created/updated events
                LOG.debugf("Ignoring account %s event: %s", event.getOperationCase(), accountId);
                yield Uni.createFrom().voidItem();
            }
            case OPERATION_NOT_SET -> {
                LOG.warnf("Received account event with no operation set: eventId=%s", event.getEventId());
                yield Uni.createFrom().voidItem();
            }
        };

        return processingUni
            .onFailure().invoke(e -> LOG.errorf(e, "Error processing account event: eventId=%s, accountId=%s",
                    event.getEventId(), accountId))
            .onFailure().recoverWithNull() // Don't throw - we want to continue processing other events
            .subscribeAsCompletionStage();
    }

    /**
     * Handle account inactivation (reactive).
     * <p>
     * Disables all datasources for the account and sets
     * status_reason="account_inactive".
     */
    private Uni<Void> handleAccountInactivated(String accountId, String reason) {
        LOG.infof("Account inactivated: accountId=%s, reason=%s", accountId, reason);

        return dataSourceRepository.listByAccount(accountId, true, 0, 0)
            .flatMap(datasources -> {
                if (datasources.isEmpty()) {
                    LOG.debugf("No datasources found for account: %s", accountId);
                    return Uni.createFrom().voidItem();
                }

                // Disable all active datasources in parallel
                List<Uni<Boolean>> disableUnis = datasources.stream()
                    .filter(ds -> ds.active)
                    .map(datasource -> {
                        LOG.infof("Disabling datasource: datasourceId=%s, accountId=%s, reason=account_inactive",
                            datasource.datasourceId, accountId);
                        return dataSourceRepository.setDataSourceStatus(datasource.datasourceId, false, "account_inactive")
                            .invoke(success -> {
                                if (success) {
                                    LOG.infof("Disabled datasource: datasourceId=%s, accountId=%s, reason=account_inactive",
                                        datasource.datasourceId, accountId);
                                } else {
                                    LOG.debugf("DataSource already inactive: datasourceId=%s", datasource.datasourceId);
                                }
                            });
                    })
                    .toList();

                if (disableUnis.isEmpty()) {
                    LOG.debugf("No active datasources to disable for account: %s", accountId);
                    return Uni.createFrom().voidItem();
                }

                return Uni.combine().all().unis(disableUnis).discardItems()
                    .invoke(() -> {
                        long disabledCount = datasources.stream().filter(ds -> ds.active).count();
                        LOG.infof("Account inactivation complete: accountId=%s, datasourcesDisabled=%d", accountId, disabledCount);
                    });
            });
    }

    /**
     * Handle account reactivation (reactive).
     * <p>
     * Re-enables datasources that were disabled due to account inactivation.
     * Only re-enables datasources with status_reason="account_inactive".
     */
    private Uni<Void> handleAccountReactivated(String accountId, String reason) {
        LOG.infof("Account reactivated: accountId=%s, reason=%s", accountId, reason);

        return dataSourceRepository.listByAccount(accountId, true, 0, 0)
            .flatMap(datasources -> {
                if (datasources.isEmpty()) {
                    LOG.debugf("No datasources found for account: %s", accountId);
                    return Uni.createFrom().voidItem();
                }

                // Re-enable datasources disabled due to account inactivation in parallel
                List<Uni<Boolean>> reactivateUnis = datasources.stream()
                    .filter(ds -> !ds.active && "account_inactive".equals(ds.statusReason))
                    .map(datasource -> {
                        LOG.infof("Re-enabling datasource: datasourceId=%s, accountId=%s",
                            datasource.datasourceId, accountId);
                        return dataSourceRepository.setDataSourceStatus(datasource.datasourceId, true, null)
                            .invoke(success -> {
                                if (success) {
                                    LOG.infof("Re-enabled datasource: datasourceId=%s, accountId=%s",
                                        datasource.datasourceId, accountId);
                                }
                            });
                    })
                    .toList();

                if (reactivateUnis.isEmpty()) {
                    LOG.debugf("No datasources eligible for re-enablement for account: %s", accountId);
                    return Uni.createFrom().voidItem();
                }

                return Uni.combine().all().unis(reactivateUnis).discardItems()
                    .invoke(() -> {
                        long reactivatedCount = datasources.stream()
                            .filter(ds -> !ds.active && "account_inactive".equals(ds.statusReason))
                            .count();
                        LOG.infof("Account reactivation complete: accountId=%s, datasourcesReactivated=%d", accountId, reactivatedCount);
                    });
            });
    }
}
