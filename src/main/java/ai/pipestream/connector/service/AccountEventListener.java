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
 * Consumes account lifecycle events and synchronizes datasource status.
 */
@ApplicationScoped
public class AccountEventListener {

    private static final Logger LOG = Logger.getLogger(AccountEventListener.class);

    @Inject
    DataSourceRepository dataSourceRepository;

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
        } catch (Exception e) {
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
