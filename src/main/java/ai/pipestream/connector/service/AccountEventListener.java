package ai.pipestream.connector.service;

import ai.pipestream.connector.entity.DataSource;
import ai.pipestream.connector.repository.DataSourceRepository;
import ai.pipestream.repository.account.v1.AccountEvent;
import io.smallrye.common.annotation.Blocking;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.logging.Logger;

import java.util.List;

/**
 * Consumes account lifecycle events and reconciles datasource access state.
 *
 * <p>Account-manager is the authority for whether an account is active.
 * connector-admin mirrors only the effect that matters for ingestion: when an
 * account is inactivated, active datasources for that account are disabled with
 * {@code status_reason=account_inactive}; when the account is reactivated, only
 * datasources disabled for that reason are re-enabled.
 *
 * <p>The method is both {@link Blocking} and {@link Transactional}. The Kafka
 * connector can invoke consumers outside an HTTP/gRPC request context, so the
 * transaction is required for Hibernate ORM access and for reliable message
 * failure reporting.
 */
@ApplicationScoped
public class AccountEventListener {

    private static final Logger LOG = Logger.getLogger(AccountEventListener.class);

    @Inject
    DataSourceRepository dataSourceRepository;

    /**
     * Applies the ingestion-relevant effect of an account lifecycle event.
     *
     * @param event account-manager lifecycle event; null records are ignored so
     *        deserialization failures do not crash the consumer
     */
    @Incoming("account-events")
    @Blocking
    @Transactional
    public void handleAccountEvent(AccountEvent event) {
        if (event == null) {
            LOG.debug("Skipping account-events record: deserialization failed (payload null)");
            return;
        }

        String accountId = event.getAccountId();
        LOG.infof("Received account event: eventId=%s, accountId=%s, operation=%s",
            event.getEventId(), accountId, event.getOperationCase());

        try {
            switch (event.getOperationCase()) {
                case INACTIVATED -> handleAccountInactivated(accountId, event.getInactivated().getReason());
                case REACTIVATED -> handleAccountReactivated(accountId, event.getReactivated().getReason());
                case CREATED, UPDATED -> LOG.debugf("Ignoring account %s event: %s", event.getOperationCase(), accountId);
                case OPERATION_NOT_SET -> LOG.warnf("Received account event with no operation set: eventId=%s", event.getEventId());
            }
        } catch (RuntimeException e) {
            LOG.errorf(e, "Error processing account event: eventId=%s, accountId=%s", event.getEventId(), accountId);
            throw e;
        }
    }

    private void handleAccountInactivated(String accountId, String reason) {
        LOG.infof("Account inactivated: accountId=%s, reason=%s", accountId, reason);
        List<DataSource> datasources = dataSourceRepository.listByAccount(accountId, true, 0, 0);
        long disabledCount = datasources.stream()
            .filter(ds -> Boolean.TRUE.equals(ds.active))
            .filter(ds -> dataSourceRepository.setDataSourceStatus(ds.datasourceId, false, "account_inactive"))
            .count();
        LOG.infof("Account inactivation complete: accountId=%s, datasourcesDisabled=%d", accountId, disabledCount);
    }

    private void handleAccountReactivated(String accountId, String reason) {
        LOG.infof("Account reactivated: accountId=%s, reason=%s", accountId, reason);
        List<DataSource> datasources = dataSourceRepository.listByAccount(accountId, true, 0, 0);
        long reactivatedCount = datasources.stream()
            .filter(ds -> !Boolean.TRUE.equals(ds.active) && "account_inactive".equals(ds.statusReason))
            .filter(ds -> dataSourceRepository.setDataSourceStatus(ds.datasourceId, true, null))
            .count();
        LOG.infof("Account reactivation complete: accountId=%s, datasourcesReactivated=%d", accountId, reactivatedCount);
    }
}
