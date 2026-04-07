package ai.pipestream.connector.service;

import ai.pipestream.connector.intake.v1.MutinyConnectorRegistrationServiceGrpc;
import ai.pipestream.connector.intake.v1.MutinyDataSourceAdminServiceGrpc;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;

/**
 * Unit tests for connector type CRUD via gRPC.
 * <p>
 * Runs in-process with {@code @QuarkusTest} and CDI-injected gRPC stubs.
 * All test logic lives in {@link ConnectorTypeCrudBaseTest}.
 */
@QuarkusTest
public class ConnectorTypeCrudTest extends ConnectorTypeCrudBaseTest {

    @GrpcClient
    MutinyConnectorRegistrationServiceGrpc.MutinyConnectorRegistrationServiceStub connectorRegistrationService;

    @GrpcClient
    MutinyDataSourceAdminServiceGrpc.MutinyDataSourceAdminServiceStub dataSourceAdminService;

    @Override
    protected MutinyConnectorRegistrationServiceGrpc.MutinyConnectorRegistrationServiceStub registrationStub() {
        return connectorRegistrationService;
    }

    @Override
    protected MutinyDataSourceAdminServiceGrpc.MutinyDataSourceAdminServiceStub adminStub() {
        return dataSourceAdminService;
    }
}
