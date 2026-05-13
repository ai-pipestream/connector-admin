package ai.pipestream.connector.service;

import ai.pipestream.connector.credentials.CredentialService;
import ai.pipestream.connector.entity.Connector;
import ai.pipestream.connector.entity.DataSource;
import ai.pipestream.connector.intake.v1.CleanupTestDataSourcesRequest;
import ai.pipestream.connector.intake.v1.CleanupTestDataSourcesResponse;
import ai.pipestream.connector.intake.v1.CreateDataSourceRequest;
import ai.pipestream.connector.intake.v1.CreateDataSourceResponse;
import ai.pipestream.connector.intake.v1.DataSourceAdminServiceGrpc;
import ai.pipestream.connector.intake.v1.DataSourceConfig;
import ai.pipestream.connector.intake.v1.DeleteDataSourceRequest;
import ai.pipestream.connector.intake.v1.DeleteDataSourceResponse;
import ai.pipestream.connector.intake.v1.GetConnectorTypeRequest;
import ai.pipestream.connector.intake.v1.GetConnectorTypeResponse;
import ai.pipestream.connector.intake.v1.GetCrawlHistoryRequest;
import ai.pipestream.connector.intake.v1.GetCrawlHistoryResponse;
import ai.pipestream.connector.intake.v1.GetDataSourceRequest;
import ai.pipestream.connector.intake.v1.GetDataSourceResponse;
import ai.pipestream.connector.intake.v1.ListConnectorTypesRequest;
import ai.pipestream.connector.intake.v1.ListConnectorTypesResponse;
import ai.pipestream.connector.intake.v1.ListDataSourcesRequest;
import ai.pipestream.connector.intake.v1.ListDataSourcesResponse;
import ai.pipestream.connector.intake.v1.RotateApiKeyRequest;
import ai.pipestream.connector.intake.v1.RotateApiKeyResponse;
import ai.pipestream.connector.intake.v1.SetDataSourceStatusRequest;
import ai.pipestream.connector.intake.v1.SetDataSourceStatusResponse;
import ai.pipestream.connector.intake.v1.UpdateDataSourceRequest;
import ai.pipestream.connector.intake.v1.UpdateDataSourceResponse;
import ai.pipestream.connector.intake.v1.ValidateApiKeyRequest;
import ai.pipestream.connector.intake.v1.ValidateApiKeyResponse;
import ai.pipestream.connector.repository.DataSourceRepository;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.quarkus.grpc.GrpcService;
import io.smallrye.common.annotation.Blocking;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.jboss.logging.Logger;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Production gRPC gateway for datasource lifecycle and credential validation.
 *
 * <p>This service is the control-plane authority for connector ingress. External
 * connectors do not write directly to the indexer: they present a datasource ID
 * and API key to connector-intake, and connector-intake calls
 * {@code ValidateApiKey} here before accepting the upload. A successful
 * validation response returns the merged {@link DataSourceConfig} that tells
 * intake how the document should be routed and persisted.
 *
 * <p>Credential handling is intentionally narrow:
 * <ul>
 *   <li>{@code CreateDataSource} and {@code RotateApiKey} are the only RPCs that
 *       return plaintext API keys.</li>
 *   <li>All read/list RPCs omit plaintext keys.</li>
 *   <li>Inactive, disabled, or soft-deleted datasources cannot validate.</li>
 * </ul>
 *
 * <p>The implementation is blocking by design. Quarkus runs this bean on worker
 * threads via {@link Blocking}, and persistence uses Hibernate ORM/Panache inside
 * transactional repository methods.
 */
@GrpcService
@Blocking
public class DataSourceAdminServiceImpl extends DataSourceAdminServiceGrpc.DataSourceAdminServiceImplBase {

    private static final Logger LOG = Logger.getLogger(DataSourceAdminServiceImpl.class);
    private static final String TEST_ACCOUNT_ID_PREFIX = "test-";

    @Inject
    DataSourceRepository dataSourceRepository;

    @Inject
    AccountValidationService accountValidationService;

    @Inject
    CredentialService apiKeyUtil;

    @Inject
    ConfigMergingService configMergingService;

    @Override
    public void createDataSource(CreateDataSourceRequest request, StreamObserver<CreateDataSourceResponse> observer) {
        respond(observer, () -> {
            validateCreateRequest(request);
            accountValidationService.validateAccountExistsAndActive(request.getAccountId());

            String apiKey = apiKeyUtil.generateApiKey();
            String apiKeyHash = apiKeyUtil.hashApiKey(apiKey);
            String metadataJson = request.getMetadataMap().isEmpty() ? "{}" : mapToJson(request.getMetadataMap());

            Connector connector = dataSourceRepository.findConnectorById(request.getConnectorId());
            if (connector == null) {
                throw Status.NOT_FOUND
                    .withDescription("Connector type not found: " + request.getConnectorId())
                    .asRuntimeException();
            }

            DataSource datasource = dataSourceRepository.createDataSource(
                request.getAccountId(),
                request.getConnectorId(),
                request.getName(),
                apiKeyHash,
                request.getDriveName(),
                metadataJson);
            if (datasource == null) {
                throw Status.ALREADY_EXISTS
                    .withDescription("DataSource already exists for account "
                        + request.getAccountId() + " and connector " + request.getConnectorId())
                    .asRuntimeException();
            }

            LOG.infof("Created datasource %s for account %s", datasource.datasourceId, request.getAccountId());
            return CreateDataSourceResponse.newBuilder()
                .setSuccess(true)
                .setDatasource(toProtoDataSourceWithApiKey(datasource, apiKey))
                .setMessage("DataSource created successfully")
                .build();
        });
    }

    @Override
    public void updateDataSource(UpdateDataSourceRequest request, StreamObserver<UpdateDataSourceResponse> observer) {
        respond(observer, () -> {
            if (request.getDatasourceId() == null || request.getDatasourceId().isEmpty()) {
                throw Status.INVALID_ARGUMENT.withDescription("DataSource ID is required").asRuntimeException();
            }

            String metadataJson = request.getMetadataMap().isEmpty() ? null : mapToJson(request.getMetadataMap());
            DataSource updated = dataSourceRepository.updateDataSource(
                request.getDatasourceId(),
                request.getName(),
                metadataJson,
                request.getDriveName());
            if (updated == null) {
                throw Status.NOT_FOUND
                    .withDescription("DataSource not found: " + request.getDatasourceId())
                    .asRuntimeException();
            }
            return UpdateDataSourceResponse.newBuilder()
                .setSuccess(true)
                .setMessage("DataSource updated successfully")
                .setDatasource(toProtoDataSource(updated))
                .build();
        });
    }

    @Override
    public void getDataSource(GetDataSourceRequest request, StreamObserver<GetDataSourceResponse> observer) {
        respond(observer, () -> {
            DataSource datasource = dataSourceRepository.findByDatasourceId(request.getDatasourceId());
            if (datasource == null) {
                throw Status.NOT_FOUND
                    .withDescription("DataSource not found: " + request.getDatasourceId())
                    .asRuntimeException();
            }
            return GetDataSourceResponse.newBuilder().setDatasource(toProtoDataSource(datasource)).build();
        });
    }

    @Override
    public void validateApiKey(ValidateApiKeyRequest request, StreamObserver<ValidateApiKeyResponse> observer) {
        respond(observer, () -> {
            DataSource datasource = dataSourceRepository.findByDatasourceId(request.getDatasourceId());
            if (datasource == null) {
                return ValidateApiKeyResponse.newBuilder()
                    .setValid(false)
                    .setMessage("DataSource not found: " + request.getDatasourceId())
                    .build();
            }
            if (!Boolean.TRUE.equals(datasource.active)) {
                return ValidateApiKeyResponse.newBuilder()
                    .setValid(false)
                    .setMessage("DataSource is inactive: " + request.getDatasourceId())
                    .build();
            }
            if (!apiKeyUtil.verifyApiKey(request.getApiKey(), datasource.apiKeyHash)) {
                return ValidateApiKeyResponse.newBuilder()
                    .setValid(false)
                    .setMessage("Invalid API key")
                    .build();
            }

            Connector connector = dataSourceRepository.findConnectorById(datasource.connectorId);
            return ValidateApiKeyResponse.newBuilder()
                .setValid(true)
                .setMessage("API key is valid")
                .setConfig(toProtoDataSourceConfig(datasource, connector))
                .build();
        });
    }

    @Override
    public void listDataSources(ListDataSourcesRequest request, StreamObserver<ListDataSourcesResponse> observer) {
        respond(observer, () -> {
            int offset = parseOffset(request.getPageToken());
            int pageSize = request.getPageSize() > 0 ? request.getPageSize() : 50;
            List<DataSource> datasources = (request.getAccountId() != null && !request.getAccountId().isEmpty())
                ? dataSourceRepository.listByAccount(request.getAccountId(), request.getIncludeInactive(), pageSize + 1, offset)
                : dataSourceRepository.listAll(request.getIncludeInactive(), pageSize + 1, offset);
            long totalCount = dataSourceRepository.countDataSources(request.getAccountId(), request.getIncludeInactive());

            String nextPageToken = "";
            List<DataSource> finalDatasources = datasources;
            if (datasources.size() > pageSize) {
                finalDatasources = datasources.subList(0, pageSize);
                nextPageToken = String.valueOf(offset + pageSize);
            }

            ListDataSourcesResponse.Builder builder = ListDataSourcesResponse.newBuilder()
                .setTotalCount((int) Math.min(totalCount, Integer.MAX_VALUE));
            if (!nextPageToken.isEmpty()) {
                builder.setNextPageToken(nextPageToken);
            }
            finalDatasources.forEach(ds -> builder.addDatasources(toProtoDataSource(ds)));
            return builder.build();
        });
    }

    @Override
    public void setDataSourceStatus(SetDataSourceStatusRequest request, StreamObserver<SetDataSourceStatusResponse> observer) {
        respond(observer, () -> {
            boolean success = dataSourceRepository.setDataSourceStatus(
                request.getDatasourceId(), request.getActive(), request.getReason());
            return SetDataSourceStatusResponse.newBuilder()
                .setSuccess(success)
                .setMessage(success ? "DataSource status updated successfully" : "DataSource not found: " + request.getDatasourceId())
                .build();
        });
    }

    @Override
    public void deleteDataSource(DeleteDataSourceRequest request, StreamObserver<DeleteDataSourceResponse> observer) {
        respond(observer, () -> {
            boolean success = dataSourceRepository.deleteDataSource(request.getDatasourceId(), "DataSource deleted via API");
            return DeleteDataSourceResponse.newBuilder()
                .setSuccess(success)
                .setMessage(success ? "DataSource deleted successfully" : "DataSource not found: " + request.getDatasourceId())
                .build();
        });
    }

    @Override
    public void rotateApiKey(RotateApiKeyRequest request, StreamObserver<RotateApiKeyResponse> observer) {
        respond(observer, () -> {
            String newApiKey = apiKeyUtil.generateApiKey();
            String newApiKeyHash = apiKeyUtil.hashApiKey(newApiKey);
            if (!dataSourceRepository.rotateApiKey(request.getDatasourceId(), newApiKeyHash)) {
                throw Status.NOT_FOUND
                    .withDescription("DataSource not found: " + request.getDatasourceId())
                    .asRuntimeException();
            }
            return RotateApiKeyResponse.newBuilder()
                .setSuccess(true)
                .setNewApiKey(newApiKey)
                .setMessage("API key rotated successfully")
                .build();
        });
    }

    @Override
    public void getCrawlHistory(GetCrawlHistoryRequest request, StreamObserver<GetCrawlHistoryResponse> observer) {
        observer.onError(Status.UNIMPLEMENTED
            .withDescription("GetCrawlHistory not yet implemented")
            .asRuntimeException());
    }

    @Override
    public void listConnectorTypes(ListConnectorTypesRequest request, StreamObserver<ListConnectorTypesResponse> observer) {
        respond(observer, () -> {
            List<Connector> connectors = dataSourceRepository.listConnectorTypes();
            ListConnectorTypesResponse.Builder builder = ListConnectorTypesResponse.newBuilder()
                .setTotalCount(connectors.size());
            connectors.forEach(c -> builder.addConnectors(toProtoConnector(c)));
            return builder.build();
        });
    }

    @Override
    public void getConnectorType(GetConnectorTypeRequest request, StreamObserver<GetConnectorTypeResponse> observer) {
        respond(observer, () -> {
            Connector connector = dataSourceRepository.findConnectorById(request.getConnectorId());
            if (connector == null) {
                throw Status.NOT_FOUND
                    .withDescription("Connector type not found: " + request.getConnectorId())
                    .asRuntimeException();
            }
            return GetConnectorTypeResponse.newBuilder().setConnector(toProtoConnector(connector)).build();
        });
    }

    @Override
    @Transactional
    public void cleanupTestDataSources(CleanupTestDataSourcesRequest request,
                                       StreamObserver<CleanupTestDataSourcesResponse> observer) {
        respond(observer, () -> {
            String accountId = request.getAccountId();
            if (accountId == null || accountId.isBlank()) {
                throw Status.INVALID_ARGUMENT.withDescription("account_id must not be blank").asRuntimeException();
            }
            if (isProductionProfile()) {
                throw Status.FAILED_PRECONDITION
                    .withDescription("cleanupTestDataSources is disabled in production profiles")
                    .asRuntimeException();
            }
            if (!isAllowedTestAccountId(accountId)) {
                throw Status.INVALID_ARGUMENT
                    .withDescription("account_id must start with \"" + TEST_ACCOUNT_ID_PREFIX + "\" for test-data cleanup")
                    .asRuntimeException();
            }

            List<String> ids = DataSource.find("select d.datasourceId from DataSource d where d.accountId = ?1", accountId)
                .project(String.class)
                .list();
            if (ids.isEmpty()) {
                return CleanupTestDataSourcesResponse.newBuilder()
                    .setSuccess(true)
                    .setMessage("No datasources found for accountId: " + accountId)
                    .setDatasourcesDeleted(0)
                    .build();
            }
            long deleteCount = DataSource.delete("datasourceId in ?1", ids);
            return CleanupTestDataSourcesResponse.newBuilder()
                .setSuccess(true)
                .setMessage("Deleted " + deleteCount + " datasource(s) for accountId: " + accountId)
                .setDatasourcesDeleted((int) Math.min(deleteCount, Integer.MAX_VALUE))
                .addAllDeletedDatasourceIds(ids)
                .build();
        });
    }

    private void validateCreateRequest(CreateDataSourceRequest request) {
        if (request.getAccountId() == null || request.getAccountId().isEmpty()) {
            throw Status.INVALID_ARGUMENT.withDescription("Account ID is required").asRuntimeException();
        }
        if (request.getConnectorId() == null || request.getConnectorId().isEmpty()) {
            throw Status.INVALID_ARGUMENT.withDescription("Connector ID is required").asRuntimeException();
        }
        if (request.getName() == null || request.getName().isEmpty()) {
            throw Status.INVALID_ARGUMENT.withDescription("Name is required").asRuntimeException();
        }
        if (request.getDriveName() == null || request.getDriveName().isEmpty()) {
            throw Status.INVALID_ARGUMENT.withDescription("Drive name is required").asRuntimeException();
        }
    }

    private int parseOffset(String pageToken) {
        if (pageToken == null || pageToken.isEmpty()) {
            return 0;
        }
        try {
            return Integer.parseInt(pageToken);
        } catch (NumberFormatException e) {
            LOG.warnf("Invalid page token '%s', using offset 0", pageToken);
            return 0;
        }
    }

    private boolean isProductionProfile() {
        String profile = System.getProperty("quarkus.profile");
        if (profile == null || profile.isBlank()) {
            profile = System.getenv("QUARKUS_PROFILE");
        }
        if (profile == null || profile.isBlank()) {
            return false;
        }
        String normalizedProfile = profile.trim().toLowerCase(java.util.Locale.ROOT);
        return "prod".equals(normalizedProfile) || "production".equals(normalizedProfile);
    }

    private boolean isAllowedTestAccountId(String accountId) {
        return accountId != null && accountId.startsWith(TEST_ACCOUNT_ID_PREFIX);
    }

    private <T> void respond(StreamObserver<T> observer, ResponseSupplier<T> supplier) {
        try {
            observer.onNext(supplier.get());
            observer.onCompleted();
        } catch (StatusRuntimeException e) {
            observer.onError(e);
        } catch (RuntimeException e) {
            LOG.error("Unhandled connector-admin gRPC failure", e);
            observer.onError(Status.UNKNOWN.withDescription(e.getMessage()).withCause(e).asRuntimeException());
        }
    }

    @FunctionalInterface
    private interface ResponseSupplier<T> {
        T get();
    }

    private ai.pipestream.connector.intake.v1.DataSource toProtoDataSource(DataSource ds) {
        return toProtoDataSourceWithApiKey(ds, "");
    }

    private ai.pipestream.connector.intake.v1.DataSource toProtoDataSourceWithApiKey(DataSource ds, String apiKey) {
        ai.pipestream.connector.intake.v1.DataSource.Builder builder =
            ai.pipestream.connector.intake.v1.DataSource.newBuilder()
                .setDatasourceId(ds.datasourceId)
                .setAccountId(ds.accountId)
                .setConnectorId(ds.connectorId)
                .setName(ds.name)
                .setApiKey(apiKey != null ? apiKey : "")
                .setDriveName(ds.driveName)
                .setActive(Boolean.TRUE.equals(ds.active));

        if (ds.metadata != null && !ds.metadata.isEmpty() && !ds.metadata.equals("{}")) {
            builder.putAllMetadata(jsonToMap(ds.metadata));
        }
        if (ds.maxFileSize != null && ds.maxFileSize > 0) {
            builder.setMaxFileSize(ds.maxFileSize);
        }
        if (ds.rateLimitPerMinute != null && ds.rateLimitPerMinute > 0) {
            builder.setRateLimitPerMinute(ds.rateLimitPerMinute);
        }
        if (ds.createdAt != null) {
            builder.setCreatedAt(Timestamp.newBuilder()
                .setSeconds(ds.createdAt.toEpochSecond())
                .setNanos(ds.createdAt.getNano())
                .build());
        }
        if (ds.updatedAt != null) {
            builder.setUpdatedAt(Timestamp.newBuilder()
                .setSeconds(ds.updatedAt.toEpochSecond())
                .setNanos(ds.updatedAt.getNano())
                .build());
        }
        return builder.build();
    }

    private DataSourceConfig toProtoDataSourceConfig(DataSource ds, Connector connector) {
        DataSourceConfig.Builder builder = DataSourceConfig.newBuilder()
            .setAccountId(ds.accountId)
            .setDatasourceId(ds.datasourceId)
            .setConnectorId(ds.connectorId)
            .setDriveName(ds.driveName)
            .setGlobalConfig(configMergingService.mergeTier1Config(connector, ds));

        if (ds.maxFileSize != null && ds.maxFileSize > 0) {
            builder.setMaxFileSize(ds.maxFileSize);
        }
        if (ds.rateLimitPerMinute != null && ds.rateLimitPerMinute > 0) {
            builder.setRateLimitPerMinute(ds.rateLimitPerMinute);
        }
        if (ds.metadata != null && !ds.metadata.isEmpty() && !ds.metadata.equals("{}")) {
            builder.putAllMetadata(jsonToMap(ds.metadata));
        }
        return builder.build();
    }

    private ai.pipestream.connector.v1.Connector toProtoConnector(Connector c) {
        ai.pipestream.connector.v1.ManagementType mgmtType =
            ai.pipestream.connector.v1.ManagementType.MANAGEMENT_TYPE_UNSPECIFIED;
        if ("MANAGED".equalsIgnoreCase(c.managementType)) {
            mgmtType = ai.pipestream.connector.v1.ManagementType.MANAGEMENT_TYPE_MANAGED;
        } else if ("UNMANAGED".equalsIgnoreCase(c.managementType)) {
            mgmtType = ai.pipestream.connector.v1.ManagementType.MANAGEMENT_TYPE_UNMANAGED;
        }

        ai.pipestream.connector.v1.Connector.Builder builder =
            ai.pipestream.connector.v1.Connector.newBuilder()
                .setConnectorId(c.connectorId)
                .setConnectorType(c.connectorType)
                .setName(c.name)
                .setManagementType(mgmtType);

        if (c.description != null) builder.setDescription(c.description);
        if (c.createdAt != null) {
            builder.setCreatedAt(Timestamp.newBuilder()
                .setSeconds(c.createdAt.toEpochSecond())
                .setNanos(c.createdAt.getNano())
                .build());
        }
        if (c.updatedAt != null) {
            builder.setUpdatedAt(Timestamp.newBuilder()
                .setSeconds(c.updatedAt.toEpochSecond())
                .setNanos(c.updatedAt.getNano())
                .build());
        }
        if (c.customConfigSchemaId != null) builder.setCustomConfigSchemaId(c.customConfigSchemaId);
        if (c.defaultPersistPipedoc != null) builder.setDefaultPersistPipedoc(c.defaultPersistPipedoc);
        if (c.defaultMaxInlineSizeBytes != null) builder.setDefaultMaxInlineSizeBytes(c.defaultMaxInlineSizeBytes);
        if (c.defaultCustomConfig != null && !c.defaultCustomConfig.isEmpty() && !c.defaultCustomConfig.equals("{}")) {
            try {
                com.google.protobuf.Struct.Builder structBuilder = com.google.protobuf.Struct.newBuilder();
                com.google.protobuf.util.JsonFormat.parser().merge(c.defaultCustomConfig, structBuilder);
                builder.setDefaultCustomConfig(structBuilder.build());
            } catch (InvalidProtocolBufferException e) {
                LOG.warnf(e, "Failed to parse connector default_custom_config for connector %s", c.connectorId);
            }
        }
        if (c.displayName != null) builder.setDisplayName(c.displayName);
        if (c.owner != null) builder.setOwner(c.owner);
        if (c.documentationUrl != null) builder.setDocumentationUrl(c.documentationUrl);
        if (c.tags != null && !c.tags.isEmpty()) builder.addAllTags(c.tags);
        return builder.build();
    }

    private String mapToJson(Map<String, String> map) {
        if (map == null || map.isEmpty()) {
            return "{}";
        }
        return "{" + map.entrySet().stream()
            .map(e -> "\"" + escapeJson(e.getKey()) + "\":\"" + escapeJson(e.getValue()) + "\"")
            .collect(Collectors.joining(",")) + "}";
    }

    private Map<String, String> jsonToMap(String json) {
        if (json == null || json.isEmpty() || json.equals("{}")) {
            return Map.of();
        }
        String content = json.trim();
        if (content.startsWith("{")) content = content.substring(1);
        if (content.endsWith("}")) content = content.substring(0, content.length() - 1);
        if (content.trim().isEmpty()) return Map.of();

        return java.util.Arrays.stream(content.split(","))
            .map(pair -> pair.split(":", 2))
            .filter(kv -> kv.length == 2)
            .collect(Collectors.toMap(kv -> unquote(kv[0].trim()), kv -> unquote(kv[1].trim())));
    }

    private String escapeJson(String s) {
        return s.replace("\\", "\\\\")
            .replace("\"", "\\\"")
            .replace("\n", "\\n")
            .replace("\r", "\\r")
            .replace("\t", "\\t");
    }

    private String unquote(String s) {
        if (s.startsWith("\"") && s.endsWith("\"")) {
            return s.substring(1, s.length() - 1);
        }
        return s;
    }
}
