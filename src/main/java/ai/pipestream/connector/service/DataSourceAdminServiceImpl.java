package ai.pipestream.connector.service;

import com.google.protobuf.Timestamp;
import io.grpc.Status;
import ai.pipestream.connector.entity.Connector;
import ai.pipestream.connector.entity.DataSource;
import ai.pipestream.connector.intake.v1.CreateDataSourceRequest;
import ai.pipestream.connector.intake.v1.CreateDataSourceResponse;
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
import ai.pipestream.connector.intake.v1.ManagementType;
import ai.pipestream.connector.intake.v1.MutinyDataSourceAdminServiceGrpc;
import ai.pipestream.connector.intake.v1.RotateApiKeyRequest;
import ai.pipestream.connector.intake.v1.RotateApiKeyResponse;
import ai.pipestream.connector.intake.v1.SetDataSourceStatusRequest;
import ai.pipestream.connector.intake.v1.SetDataSourceStatusResponse;
import ai.pipestream.connector.intake.v1.UpdateDataSourceRequest;
import ai.pipestream.connector.intake.v1.UpdateDataSourceResponse;
import ai.pipestream.connector.intake.v1.ValidateApiKeyRequest;
import ai.pipestream.connector.intake.v1.ValidateApiKeyResponse;
import ai.pipestream.connector.repository.DataSourceRepository;
import ai.pipestream.connector.util.ApiKeyUtil;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * gRPC service implementation for DataSource Administration.
 * <p>
 * Provides datasource lifecycle and administration APIs:
 * <ul>
 *   <li>DataSource creation with API key generation</li>
 *   <li>Lookup and listing with pagination</li>
 *   <li>Status transitions (enable/disable) and soft deletion</li>
 *   <li>API key rotation using Argon2id hashing via {@link ApiKeyUtil}</li>
 *   <li>Connector type listing (pre-seeded templates)</li>
 * </ul>
 *
 * Proto Definition: intake/proto/ai/pipestream/connector/intake/v1/connector_intake_service.proto
 */
@GrpcService
public class DataSourceAdminServiceImpl extends MutinyDataSourceAdminServiceGrpc.DataSourceAdminServiceImplBase {

    private static final Logger LOG = Logger.getLogger(DataSourceAdminServiceImpl.class);

    @Inject
    DataSourceRepository dataSourceRepository;

    @Inject
    AccountValidationService accountValidationService;

    @Inject
    ApiKeyUtil apiKeyUtil;

    /**
     * Creates a new datasource for an account.
     *
     * @param request CreateDataSourceRequest containing account_id, connector_id, name, drive_name
     * @return CreateDataSourceResponse with datasource_id and api_key (returned once)
     */
    @Override
    public Uni<CreateDataSourceResponse> createDataSource(CreateDataSourceRequest request) {
        return Uni.createFrom().item(() -> {
            LOG.infof("Creating datasource: account=%s, connector=%s, name=%s",
                request.getAccountId(), request.getConnectorId(), request.getName());

            // Validate required fields
            if (request.getAccountId() == null || request.getAccountId().isEmpty()) {
                throw new IllegalArgumentException("Account ID is required");
            }
            if (request.getConnectorId() == null || request.getConnectorId().isEmpty()) {
                throw new IllegalArgumentException("Connector ID is required");
            }
            if (request.getName() == null || request.getName().isEmpty()) {
                throw new IllegalArgumentException("Name is required");
            }
            if (request.getDriveName() == null || request.getDriveName().isEmpty()) {
                throw new IllegalArgumentException("Drive name is required");
            }

            // Verify connector type exists
            Connector connector = dataSourceRepository.findConnectorById(request.getConnectorId());
            if (connector == null) {
                throw Status.NOT_FOUND
                    .withDescription("Connector type not found: " + request.getConnectorId())
                    .asRuntimeException();
            }

            return request;
        })
        .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
        // Validate account exists and is active via gRPC
        .flatMap(req -> accountValidationService.validateAccountExistsAndActive(req.getAccountId())
            .replaceWith(req))
        // Create datasource with generated API key
        .flatMap(req -> Uni.createFrom().item(() -> {
            // Generate API key
            String apiKey = apiKeyUtil.generateApiKey();
            String apiKeyHash = apiKeyUtil.hashApiKey(apiKey);

            // Build metadata JSON from request
            String metadataJson = "{}";
            if (!req.getMetadataMap().isEmpty()) {
                metadataJson = mapToJson(req.getMetadataMap());
            }

            // Create datasource
            DataSource datasource = dataSourceRepository.createDataSource(
                req.getAccountId(),
                req.getConnectorId(),
                req.getName(),
                apiKeyHash,
                req.getDriveName(),
                metadataJson
            );

            if (datasource == null) {
                throw Status.ALREADY_EXISTS
                    .withDescription("DataSource already exists for account " + req.getAccountId() +
                                     " and connector " + req.getConnectorId())
                    .asRuntimeException();
            }

            LOG.infof("Created datasource %s for account %s with connector %s",
                datasource.datasourceId, req.getAccountId(), req.getConnectorId());

            // Build a DataSource proto with API key included (only returned at creation time)
            ai.pipestream.connector.intake.v1.DataSource dsProto = toProtoDataSourceWithApiKey(datasource, apiKey);

            return CreateDataSourceResponse.newBuilder()
                .setSuccess(true)
                .setDatasource(dsProto)
                .setMessage("DataSource created successfully")
                .build();
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool()));
    }

    /**
     * Updates a datasource.
     *
     * @param request UpdateDataSourceRequest with datasource_id and optional fields
     * @return UpdateDataSourceResponse with updated datasource
     */
    @Override
    public Uni<UpdateDataSourceResponse> updateDataSource(UpdateDataSourceRequest request) {
        return Uni.createFrom().item(() -> {
            LOG.infof("Updating datasource: %s", request.getDatasourceId());

            if (request.getDatasourceId() == null || request.getDatasourceId().isEmpty()) {
                throw new IllegalArgumentException("DataSource ID is required");
            }

            String metadataJson = null;
            if (!request.getMetadataMap().isEmpty()) {
                metadataJson = mapToJson(request.getMetadataMap());
            }

            DataSource updated = dataSourceRepository.updateDataSource(
                request.getDatasourceId(),
                request.getName(),
                metadataJson,
                request.getDriveName()
            );

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
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    /**
     * Gets a datasource by ID.
     *
     * @param request GetDataSourceRequest with datasource_id
     * @return GetDataSourceResponse with datasource details
     */
    @Override
    public Uni<GetDataSourceResponse> getDataSource(GetDataSourceRequest request) {
        return Uni.createFrom().item(() -> {
            LOG.infof("Getting datasource: %s", request.getDatasourceId());

            DataSource datasource = dataSourceRepository.findByDatasourceId(request.getDatasourceId());
            if (datasource == null) {
                throw Status.NOT_FOUND
                    .withDescription("DataSource not found: " + request.getDatasourceId())
                    .asRuntimeException();
            }

            return GetDataSourceResponse.newBuilder()
                .setDatasource(toProtoDataSource(datasource))
                .build();
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    /**
     * Validates an API key for a datasource.
     *
     * @param request ValidateApiKeyRequest with datasource_id and api_key
     * @return ValidateApiKeyResponse with validation result
     */
    @Override
    public Uni<ValidateApiKeyResponse> validateApiKey(ValidateApiKeyRequest request) {
        return Uni.createFrom().item(() -> {
            LOG.debugf("Validating API key for datasource: %s", request.getDatasourceId());

            DataSource datasource = dataSourceRepository.findByDatasourceId(request.getDatasourceId());
            if (datasource == null) {
                return ValidateApiKeyResponse.newBuilder()
                    .setValid(false)
                    .setMessage("DataSource not found: " + request.getDatasourceId())
                    .build();
            }

            if (!datasource.active) {
                return ValidateApiKeyResponse.newBuilder()
                    .setValid(false)
                    .setMessage("DataSource is inactive: " + request.getDatasourceId())
                    .build();
            }

            // Verify API key
            boolean valid = apiKeyUtil.verifyApiKey(request.getApiKey(), datasource.apiKeyHash);

            if (valid) {
                LOG.debugf("API key validated successfully for datasource: %s", request.getDatasourceId());
                return ValidateApiKeyResponse.newBuilder()
                    .setValid(true)
                    .setMessage("API key is valid")
                    .setConfig(toProtoDataSourceConfig(datasource))
                    .build();
            } else {
                LOG.warnf("API key validation failed for datasource: %s", request.getDatasourceId());
                return ValidateApiKeyResponse.newBuilder()
                    .setValid(false)
                    .setMessage("Invalid API key")
                    .build();
            }
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    /**
     * Lists datasources with optional account filter and pagination.
     *
     * @param request ListDataSourcesRequest with optional filters
     * @return ListDataSourcesResponse with datasources list
     */
    @Override
    public Uni<ListDataSourcesResponse> listDataSources(ListDataSourcesRequest request) {
        return Uni.createFrom().item(() -> {
            LOG.debugf("Listing datasources: account=%s, includeInactive=%s",
                request.getAccountId(), request.getIncludeInactive());

            // Parse page token as offset
            int offset = 0;
            if (request.getPageToken() != null && !request.getPageToken().isEmpty()) {
                try {
                    offset = Integer.parseInt(request.getPageToken());
                } catch (NumberFormatException e) {
                    LOG.warnf("Invalid page token '%s', using offset 0", request.getPageToken());
                }
            }

            int pageSize = request.getPageSize() > 0 ? request.getPageSize() : 50;

            List<DataSource> datasources;
            if (request.getAccountId() != null && !request.getAccountId().isEmpty()) {
                datasources = dataSourceRepository.listByAccount(
                    request.getAccountId(),
                    request.getIncludeInactive(),
                    pageSize + 1,
                    offset
                );
            } else {
                datasources = dataSourceRepository.listAll(
                    request.getIncludeInactive(),
                    pageSize + 1,
                    offset
                );
            }

            // Determine next page token
            String nextPageToken = "";
            if (datasources.size() > pageSize) {
                datasources = datasources.subList(0, pageSize);
                nextPageToken = String.valueOf(offset + pageSize);
            }

            // Get total count
            long totalCount = dataSourceRepository.countDataSources(
                request.getAccountId(),
                request.getIncludeInactive()
            );

            ListDataSourcesResponse.Builder builder = ListDataSourcesResponse.newBuilder()
                .setTotalCount((int) Math.min(totalCount, Integer.MAX_VALUE));

            if (!nextPageToken.isEmpty()) {
                builder.setNextPageToken(nextPageToken);
            }

            for (DataSource ds : datasources) {
                builder.addDatasources(toProtoDataSource(ds));
            }

            return builder.build();
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    /**
     * Sets the active status of a datasource.
     *
     * @param request SetDataSourceStatusRequest with datasource_id and active flag
     * @return SetDataSourceStatusResponse with result
     */
    @Override
    public Uni<SetDataSourceStatusResponse> setDataSourceStatus(SetDataSourceStatusRequest request) {
        return Uni.createFrom().item(() -> {
            LOG.infof("Setting datasource %s status to active=%s",
                request.getDatasourceId(), request.getActive());

            boolean success = dataSourceRepository.setDataSourceStatus(
                request.getDatasourceId(),
                request.getActive(),
                request.getReason()
            );

            if (!success) {
                return SetDataSourceStatusResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("DataSource not found: " + request.getDatasourceId())
                    .build();
            }

            return SetDataSourceStatusResponse.newBuilder()
                .setSuccess(true)
                .setMessage("DataSource status updated successfully")
                .build();
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    /**
     * Deletes a datasource (soft delete).
     *
     * @param request DeleteDataSourceRequest with datasource_id
     * @return DeleteDataSourceResponse with result
     */
    @Override
    public Uni<DeleteDataSourceResponse> deleteDataSource(DeleteDataSourceRequest request) {
        return Uni.createFrom().item(() -> {
            LOG.infof("Deleting datasource: %s", request.getDatasourceId());

            String reason = "DataSource deleted via API";
            boolean success = dataSourceRepository.deleteDataSource(request.getDatasourceId(), reason);

            if (!success) {
                return DeleteDataSourceResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("DataSource not found: " + request.getDatasourceId())
                    .build();
            }

            return DeleteDataSourceResponse.newBuilder()
                .setSuccess(true)
                .setMessage("DataSource deleted successfully")
                .build();
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    /**
     * Rotates the API key for a datasource.
     *
     * @param request RotateApiKeyRequest with datasource_id
     * @return RotateApiKeyResponse with new API key (returned once)
     */
    @Override
    public Uni<RotateApiKeyResponse> rotateApiKey(RotateApiKeyRequest request) {
        return Uni.createFrom().item(() -> {
            LOG.infof("Rotating API key for datasource: %s", request.getDatasourceId());

            // Generate new API key
            String newApiKey = apiKeyUtil.generateApiKey();
            String newApiKeyHash = apiKeyUtil.hashApiKey(newApiKey);

            boolean success = dataSourceRepository.rotateApiKey(request.getDatasourceId(), newApiKeyHash);

            if (!success) {
                throw Status.NOT_FOUND
                    .withDescription("DataSource not found: " + request.getDatasourceId())
                    .asRuntimeException();
            }

            LOG.infof("Rotated API key for datasource %s", request.getDatasourceId());

            return RotateApiKeyResponse.newBuilder()
                .setSuccess(true)
                .setNewApiKey(newApiKey)  // Return plaintext key ONCE
                .setMessage("API key rotated successfully")
                .build();
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    /**
     * Gets crawl history for a datasource.
     *
     * @param request GetCrawlHistoryRequest
     * @return GetCrawlHistoryResponse (not yet implemented)
     */
    @Override
    public Uni<GetCrawlHistoryResponse> getCrawlHistory(GetCrawlHistoryRequest request) {
        return Uni.createFrom().failure(
            Status.UNIMPLEMENTED
                .withDescription("GetCrawlHistory not yet implemented")
                .asRuntimeException()
        );
    }

    /**
     * Lists available connector types (pre-seeded templates).
     *
     * @param request ListConnectorTypesRequest
     * @return ListConnectorTypesResponse with connector types
     */
    @Override
    public Uni<ListConnectorTypesResponse> listConnectorTypes(ListConnectorTypesRequest request) {
        return Uni.createFrom().item(() -> {
            LOG.debugf("Listing connector types");

            List<Connector> connectors = dataSourceRepository.listConnectorTypes();

            ListConnectorTypesResponse.Builder builder = ListConnectorTypesResponse.newBuilder()
                .setTotalCount(connectors.size());

            for (Connector c : connectors) {
                builder.addConnectors(toProtoConnector(c));
            }

            return builder.build();
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    /**
     * Gets connector type details.
     *
     * @param request GetConnectorTypeRequest with connector_id
     * @return GetConnectorTypeResponse with connector details
     */
    @Override
    public Uni<GetConnectorTypeResponse> getConnectorType(GetConnectorTypeRequest request) {
        return Uni.createFrom().item(() -> {
            LOG.debugf("Getting connector type: %s", request.getConnectorId());

            Connector connector = dataSourceRepository.findConnectorById(request.getConnectorId());
            if (connector == null) {
                throw Status.NOT_FOUND
                    .withDescription("Connector type not found: " + request.getConnectorId())
                    .asRuntimeException();
            }

            return GetConnectorTypeResponse.newBuilder()
                .setConnector(toProtoConnector(connector))
                .build();
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    // ========================================================================
    // Helper Methods
    // ========================================================================

    /**
     * Convert DataSource entity to proto DataSource.
     * API key is never exposed in this version.
     */
    private ai.pipestream.connector.intake.v1.DataSource toProtoDataSource(DataSource ds) {
        return toProtoDataSourceWithApiKey(ds, "");  // Never expose API key
    }

    /**
     * Convert DataSource entity to proto DataSource with API key.
     * Only used during creation when the API key needs to be returned once.
     */
    private ai.pipestream.connector.intake.v1.DataSource toProtoDataSourceWithApiKey(DataSource ds, String apiKey) {
        ai.pipestream.connector.intake.v1.DataSource.Builder builder =
            ai.pipestream.connector.intake.v1.DataSource.newBuilder()
                .setDatasourceId(ds.datasourceId)
                .setAccountId(ds.accountId)
                .setConnectorId(ds.connectorId)
                .setName(ds.name)
                .setApiKey(apiKey != null ? apiKey : "")
                .setDriveName(ds.driveName)
                .setActive(ds.active != null ? ds.active : false);

        // Add metadata if present
        if (ds.metadata != null && !ds.metadata.isEmpty() && !ds.metadata.equals("{}")) {
            Map<String, String> metadata = jsonToMap(ds.metadata);
            builder.putAllMetadata(metadata);
        }

        // Add limits directly to DataSource proto
        if (ds.maxFileSize != null && ds.maxFileSize > 0) {
            builder.setMaxFileSize(ds.maxFileSize);
        }
        if (ds.rateLimitPerMinute != null && ds.rateLimitPerMinute > 0) {
            builder.setRateLimitPerMinute(ds.rateLimitPerMinute);
        }

        // Add timestamps
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

    /**
     * Convert DataSource entity to DataSourceConfig proto.
     * Lightweight config for runtime validation responses.
     */
    private DataSourceConfig toProtoDataSourceConfig(DataSource ds) {
        DataSourceConfig.Builder builder = DataSourceConfig.newBuilder()
            .setAccountId(ds.accountId)
            .setDatasourceId(ds.datasourceId)
            .setConnectorId(ds.connectorId)
            .setDriveName(ds.driveName);

        // Add limits if set
        if (ds.maxFileSize != null && ds.maxFileSize > 0) {
            builder.setMaxFileSize(ds.maxFileSize);
        }
        if (ds.rateLimitPerMinute != null && ds.rateLimitPerMinute > 0) {
            builder.setRateLimitPerMinute(ds.rateLimitPerMinute);
        }

        // Add metadata if present
        if (ds.metadata != null && !ds.metadata.isEmpty() && !ds.metadata.equals("{}")) {
            Map<String, String> metadata = jsonToMap(ds.metadata);
            builder.putAllMetadata(metadata);
        }

        return builder.build();
    }

    /**
     * Convert Connector entity to proto Connector.
     */
    private ai.pipestream.connector.intake.v1.Connector toProtoConnector(Connector c) {
        ManagementType mgmtType = ManagementType.MANAGEMENT_TYPE_UNSPECIFIED;
        if ("MANAGED".equalsIgnoreCase(c.managementType)) {
            mgmtType = ManagementType.MANAGEMENT_TYPE_MANAGED;
        } else if ("UNMANAGED".equalsIgnoreCase(c.managementType)) {
            mgmtType = ManagementType.MANAGEMENT_TYPE_UNMANAGED;
        }

        ai.pipestream.connector.intake.v1.Connector.Builder builder =
            ai.pipestream.connector.intake.v1.Connector.newBuilder()
                .setConnectorId(c.connectorId)
                .setConnectorType(c.connectorType)
                .setName(c.name)
                .setManagementType(mgmtType);

        if (c.description != null) {
            builder.setDescription(c.description);
        }

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

        return builder.build();
    }

    /**
     * Convert map to simple JSON string.
     */
    private String mapToJson(Map<String, String> map) {
        if (map == null || map.isEmpty()) {
            return "{}";
        }
        return "{" + map.entrySet().stream()
            .map(e -> "\"" + escapeJson(e.getKey()) + "\":\"" + escapeJson(e.getValue()) + "\"")
            .collect(Collectors.joining(",")) + "}";
    }

    /**
     * Convert simple JSON string to map.
     */
    private Map<String, String> jsonToMap(String json) {
        // Simple JSON parsing for flat key-value maps
        if (json == null || json.isEmpty() || json.equals("{}")) {
            return Map.of();
        }

        // Remove braces and parse
        String content = json.trim();
        if (content.startsWith("{")) {
            content = content.substring(1);
        }
        if (content.endsWith("}")) {
            content = content.substring(0, content.length() - 1);
        }

        if (content.trim().isEmpty()) {
            return Map.of();
        }

        // Simple split by comma (doesn't handle nested objects)
        return java.util.Arrays.stream(content.split(","))
            .map(pair -> pair.split(":", 2))
            .filter(kv -> kv.length == 2)
            .collect(Collectors.toMap(
                kv -> unquote(kv[0].trim()),
                kv -> unquote(kv[1].trim())
            ));
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
