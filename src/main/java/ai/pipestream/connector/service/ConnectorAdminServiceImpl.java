package ai.pipestream.connector.service;

import com.google.protobuf.Timestamp;
import io.grpc.Status;
import ai.pipestream.connector.entity.Connector;
import ai.pipestream.connector.intake.v1.ConnectorRegistration;
import ai.pipestream.connector.util.ConnectorMetadata;
import ai.pipestream.connector.intake.v1.DeleteConnectorRequest;
import ai.pipestream.connector.intake.v1.DeleteConnectorResponse;
import ai.pipestream.connector.intake.v1.GetConnectorRequest;
import ai.pipestream.connector.intake.v1.ListConnectorsRequest;
import ai.pipestream.connector.intake.v1.ListConnectorsResponse;
import ai.pipestream.connector.intake.v1.MutinyConnectorAdminServiceGrpc;
import ai.pipestream.connector.intake.v1.RegisterConnectorRequest;
import ai.pipestream.connector.intake.v1.RegisterConnectorResponse;
import ai.pipestream.connector.intake.v1.RotateApiKeyRequest;
import ai.pipestream.connector.intake.v1.RotateApiKeyResponse;
import ai.pipestream.connector.intake.v1.SetConnectorStatusRequest;
import ai.pipestream.connector.intake.v1.SetConnectorStatusResponse;
import ai.pipestream.connector.intake.v1.UpdateConnectorRequest;
import ai.pipestream.connector.intake.v1.UpdateConnectorResponse;
import ai.pipestream.connector.intake.v1.ValidateApiKeyRequest;
import ai.pipestream.connector.intake.v1.ValidateApiKeyResponse;
import ai.pipestream.connector.intake.v1.GetConnectorResponse;
import ai.pipestream.connector.repository.ConnectorRepository;
import ai.pipestream.connector.util.ApiKeyUtil;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.List;

/**
 * gRPC service implementation for Connector Administration.
 * <p>
 * Provides connector lifecycle and administration APIs:
 * <ul>
 *   <li>Registration with API key generation and initial account linkage</li>
 *   <li>Lookup and listing with pagination</li>
 *   <li>Status transitions (enable/disable) and soft deletion</li>
 *   <li>API key rotation using Argon2id hashing via {@link ApiKeyUtil}</li>
 *   <li>Connector metadata updates (S3 pathing, limits, defaults)</li>
 * </ul>
 *
 * Reactive semantics:
 * <ul>
 *   <li>All endpoints return a cold {@code Uni} and execute work on the default worker pool via {@code runSubscriptionOn}.</li>
 *   <li>DB interactions are performed in repository methods; the gRPC event loop is never blocked.</li>
 *   <li>Errors are propagated as {@code StatusRuntimeException} with appropriate gRPC codes as noted per method.</li>
 * </ul>
 *
 * Side effects:
 * <ul>
 *   <li>Reads and writes to the connectors tables through {@link ConnectorRepository}.</li>
 *   <li>Remote gRPC calls to account-manager for account validation via {@link AccountValidationService}.</li>
 *   <li>Security-sensitive operations (API key generation and hashing).</li>
 * </ul>
 *
 * Proto Definition: grpc/grpc-stubs/src/main/proto/module/connectors/connector_intake_service.proto
 */
@GrpcService
public class ConnectorAdminServiceImpl extends MutinyConnectorAdminServiceGrpc.ConnectorAdminServiceImplBase {

    private static final Logger LOG = Logger.getLogger(ConnectorAdminServiceImpl.class);

    @Inject
    ConnectorRepository connectorRepository;

    @Inject
    AccountValidationService accountValidationService;

    @Inject
    ApiKeyUtil apiKeyUtil;

    /**
     * Registers a new connector and returns its initial API key.
     *
     * Reactive semantics:
     * - Returns a cold `Uni` that runs on the default worker pool due to repository access.
     * - Validates the target account via a remote gRPC call before DB writes.
     *
     * Side effects:
     * - Generates and hashes an API key (Argon2id) and persists the connector.
     * - Links the connector to the provided account.
     * - Emits INFO logs for auditing.
     *
     * @param request Registration parameters including `connector_name`, `connector_type`, `account_id`, and optional metadata.
     * @return `Uni` emitting a `RegisterConnectorResponse` whose `api_key` contains the plaintext key once; subsequent reads never return the key.
     * @throws io.grpc.StatusRuntimeException on error:
     *         ALREADY_EXISTS when the connector name is taken; INVALID_ARGUMENT for missing/invalid fields or when account does not exist/is inactive; other upstream errors are propagated from account-manager.
     */
    @Override
    public Uni<RegisterConnectorResponse> registerConnector(RegisterConnectorRequest request) {
        return Uni.createFrom().item(() -> {
            LOG.infof("Registering connector: name=%s, type=%s, account=%s",
                request.getConnectorName(), request.getConnectorType(), request.getAccountId());

            // Validate required fields
            if (request.getConnectorName() == null || request.getConnectorName().isEmpty()) {
                throw new IllegalArgumentException("Connector name is required");
            }
            if (request.getConnectorType() == null || request.getConnectorType().isEmpty()) {
                throw new IllegalArgumentException("Connector type is required");
            }
            if (request.getAccountId() == null || request.getAccountId().isEmpty()) {
                throw new IllegalArgumentException("Account ID is required");
            }

            // Check if connector name already exists
            Connector existing = connectorRepository.findByConnectorName(request.getConnectorName());
            if (existing != null) {
                throw Status.ALREADY_EXISTS
                    .withDescription("Connector name already exists: " + request.getConnectorName())
                    .asRuntimeException();
            }

            return request;
        })
        .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
        // Validate account exists and is active via gRPC
        .flatMap(req -> accountValidationService.validateAccountExistsAndActive(req.getAccountId())
            .replaceWith(req))
        // Create connector with generated API key
        .flatMap(req -> Uni.createFrom().item(() -> {
            // Generate API key
            String apiKey = apiKeyUtil.generateApiKey();
            String apiKeyHash = apiKeyUtil.hashApiKey(apiKey);

            // Build metadata JSON
            ConnectorMetadata metadata = new ConnectorMetadata();
            metadata.setS3Bucket(req.getS3Bucket());
            metadata.setS3BasePath(req.getS3BasePath());
            metadata.setMaxFileSize(req.getMaxFileSize());
            metadata.setRateLimitPerMinute(req.getRateLimitPerMinute());
            metadata.setDefaultMetadata(new java.util.HashMap<>(req.getDefaultMetadataMap()));
            String metadataJson = metadata.toJson();

            // Create connector with metadata
            Connector connector = connectorRepository.createConnectorWithMetadata(
                req.getConnectorName(),
                req.getConnectorType(),
                "",  // Description not in RegisterConnectorRequest proto
                apiKeyHash,
                metadataJson
            );

            // Link to account
            connectorRepository.linkConnectorToAccount(connector.connectorId, req.getAccountId());

            LOG.infof("Registered connector %s for account %s with S3 bucket: %s",
                connector.connectorId, req.getAccountId(), req.getS3Bucket());

            return RegisterConnectorResponse.newBuilder()
                .setSuccess(true)
                .setConnectorId(connector.connectorId)
                .setApiKey(apiKey)  // Return plaintext key ONCE
                .setMessage("Connector registered successfully")
                .build();
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool()));
    }

    /**
     * Returns connector details by ID.
     *
     * Reactive semantics:
     * - Cold `Uni` executing on the default worker pool to perform repository access.
     *
     * Side effects:
     * - Read-only DB access; emits INFO logs.
     *
     * @param request Request containing `connector_id`.
     * @return `Uni` emitting a `GetConnectorResponse` whose `connector` holds the registration details if found.
     * @throws io.grpc.StatusRuntimeException NOT_FOUND when the connector does not exist.
     */
    @Override
    public Uni<GetConnectorResponse> getConnector(GetConnectorRequest request) {
        return Uni.createFrom().item(() -> {
            LOG.infof("Getting connector: %s", request.getConnectorId());

            Connector connector = connectorRepository.findByConnectorId(request.getConnectorId());
            if (connector == null) {
                throw Status.NOT_FOUND
                    .withDescription("Connector not found: " + request.getConnectorId())
                    .asRuntimeException();
            }

            return GetConnectorResponse.newBuilder()
                .setConnector(toProtoConnectorRegistration(connector))
                .build();
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    /**
     * Validates a connector's API key and optionally returns connector details.
     *
     * Reactive semantics:
     * - Cold `Uni` executing on the default worker pool for repository reads and hashing.
     * - Does not perform remote calls; verification is local against the stored hash.
     *
     * Side effects:
     * - None beyond read-only DB access and logging.
     *
     * @param request Request containing `connector_id` and `api_key` to validate.
     * @return `Uni` emitting `ValidateApiKeyResponse` with `valid=true` when the key matches and the connector is active; `valid=false` with message otherwise.
     */
    @Override
    public Uni<ValidateApiKeyResponse> validateApiKey(ValidateApiKeyRequest request) {
        return Uni.createFrom().item(() -> {
            LOG.debugf("Validating API key for connector: %s", request.getConnectorId());

            Connector connector = connectorRepository.findByConnectorId(request.getConnectorId());
            if (connector == null) {
                return ValidateApiKeyResponse.newBuilder()
                    .setValid(false)
                    .setMessage("Connector not found: " + request.getConnectorId())
                    .build();
            }

            if (!connector.active) {
                return ValidateApiKeyResponse.newBuilder()
                    .setValid(false)
                    .setMessage("Connector is inactive: " + request.getConnectorId())
                    .build();
            }

            // Verify API key
            boolean valid = apiKeyUtil.verifyApiKey(request.getApiKey(), connector.apiKeyHash);
            
            if (valid) {
                LOG.debugf("API key validated successfully for connector: %s", request.getConnectorId());
                return ValidateApiKeyResponse.newBuilder()
                    .setValid(true)
                    .setMessage("API key is valid")
                    .setConnector(toProtoConnectorRegistration(connector))
                    .build();
            } else {
                LOG.warnf("API key validation failed for connector: %s", request.getConnectorId());
                return ValidateApiKeyResponse.newBuilder()
                    .setValid(false)
                    .setMessage("Invalid API key")
                    .build();
            }
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    /**
     * Lists connectors with optional account filter and pagination.
     *
     * Reactive semantics:
     * - Cold `Uni` running on the default worker pool for repository access.
     * - Pagination is offset-based; `page_token` is treated as an integer offset.
     *
     * Side effects:
     * - Read-only DB queries; emits DEBUG/INFO logs.
     *
     * @param request The request containing optional `account_id`, `include_inactive`, `page_size`, and `page_token`.
     * @return `Uni` emitting a `ListConnectorsResponse` that includes `connectors`, `total_count`, and an optional `next_page_token` when more data exists.
     */
    @Override
    public Uni<ListConnectorsResponse> listConnectors(ListConnectorsRequest request) {
        return Uni.createFrom().item(() -> {
            LOG.debugf("Listing connectors: account=%s, includeInactive=%s, pageSize=%d, pageToken=%s",
                request.getAccountId(), request.getIncludeInactive(),
                request.getPageSize(), request.getPageToken());

            // Parse page token as offset
            int offset = 0;
            if (request.getPageToken() != null && !request.getPageToken().isEmpty()) {
                try {
                    offset = Integer.parseInt(request.getPageToken());
                } catch (NumberFormatException e) {
                    LOG.warnf("Invalid page token '%s', using offset 0", request.getPageToken());
                }
            }

            // Default page size
            int pageSize = request.getPageSize() > 0 ? request.getPageSize() : 50;

            // Fetch one extra to determine if there's a next page
            List<Connector> connectors = connectorRepository.listConnectors(
                request.getAccountId(),
                request.getIncludeInactive(),
                pageSize + 1,
                offset
            );

            // Determine next page token
            String nextPageToken = "";
            if (connectors.size() > pageSize) {
                connectors = connectors.subList(0, pageSize);
                nextPageToken = String.valueOf(offset + pageSize);
            }

            // Get total count for pagination metadata
            long totalCount = connectorRepository.countConnectors(
                request.getAccountId(),
                request.getIncludeInactive()
            );

            // Build response
            ListConnectorsResponse.Builder builder = ListConnectorsResponse.newBuilder()
                .setTotalCount((int) Math.min(totalCount, Integer.MAX_VALUE));

            if (!nextPageToken.isEmpty()) {
                builder.setNextPageToken(nextPageToken);
            }

            for (Connector connector : connectors) {
                builder.addConnectors(toProtoConnectorRegistration(connector));
            }

            return builder.build();
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    /**
     * Sets the active status of a connector.
     *
     * Reactive semantics:
     * - Cold `Uni` running on the default worker pool for repository updates.
     *
     * Side effects:
     * - Writes to the connectors table; updates `active` and `status_reason` fields.
     * - Emits INFO logs for auditing.
     *
     * @param request Contains `connector_id`, desired `active` flag, and optional `reason`.
     * @return `Uni` emitting `SetConnectorStatusResponse` with `success=true` on update; `success=false` when the connector is not found.
     */
    @Override
    public Uni<SetConnectorStatusResponse> setConnectorStatus(SetConnectorStatusRequest request) {
        return Uni.createFrom().item(() -> {
            LOG.infof("Setting connector %s status to active=%s", request.getConnectorId(), request.getActive());

            boolean success = connectorRepository.setConnectorStatus(
                request.getConnectorId(),
                request.getActive(),
                request.getReason()
            );

            if (!success) {
                return SetConnectorStatusResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Connector not found: " + request.getConnectorId())
                    .build();
            }

            return SetConnectorStatusResponse.newBuilder()
                .setSuccess(true)
                .setMessage("Connector status updated successfully")
                .build();
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    /**
     * Deletes a connector using soft delete semantics.
     *
     * Reactive semantics:
     * - Cold `Uni` running on the default worker pool for repository updates.
     *
     * Side effects:
     * - Sets `active=false`, records `deleted_reason` and `deleted_at`.
     * - Does not remove crawl sessions yet (hard delete TBD).
     *
     * @param request Request containing `connector_id`; `hard_delete` is currently ignored and treated as soft delete.
     * @return `Uni` emitting `DeleteConnectorResponse` with `success=false` when the connector is not found; `crawl_sessions_deleted` is 0 for now.
     */
    @Override
    public Uni<DeleteConnectorResponse> deleteConnector(DeleteConnectorRequest request) {
        return Uni.createFrom().item(() -> {
            LOG.infof("Deleting connector: %s (hard=%s, reason=%s)",
                request.getConnectorId(), request.getHardDelete(),
                request.getHardDelete() ? "N/A" : "Soft delete");

            // MVP: Only soft delete (set active=false with reason tracking)
            // Use a default reason if none provided (hard_delete doesn't have reason in proto)
            String reason = "Connector deleted via API";
            boolean success = connectorRepository.deleteConnector(request.getConnectorId(), reason);

            if (!success) {
                return DeleteConnectorResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Connector not found: " + request.getConnectorId())
                    .setCrawlSessionsDeleted(0)
                    .build();
            }

            // TODO: Implement hard delete and crawl session cleanup

            return DeleteConnectorResponse.newBuilder()
                .setSuccess(true)
                .setMessage("Connector deleted successfully")
                .setCrawlSessionsDeleted(0)
                .build();
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    /**
     * Rotates the API key for a connector and returns the new plaintext key once.
     *
     * Reactive semantics:
     * - Cold `Uni` running on the default worker pool for hashing and repository updates.
     * - No remote calls; operation is local to this service.
     *
     * Side effects:
     * - Persists a new Argon2id hash and updates `last_rotated_at` and `updated_at`.
     * - Existing keys may remain valid for a grace period in future versions (TBD).
     *
     * @param request Contains `connector_id` to rotate.
     * @return `Uni` emitting `RotateApiKeyResponse` whose `new_api_key` contains the plaintext key once; it cannot be retrieved later.
     * @throws io.grpc.StatusRuntimeException NOT_FOUND when the connector does not exist.
     */
    @Override
    public Uni<RotateApiKeyResponse> rotateApiKey(RotateApiKeyRequest request) {
        return Uni.createFrom().item(() -> {
            LOG.infof("Rotating API key for connector: %s", request.getConnectorId());

            // Generate new API key
            String newApiKey = apiKeyUtil.generateApiKey();
            String newApiKeyHash = apiKeyUtil.hashApiKey(newApiKey);

            // Update connector
            boolean success = connectorRepository.rotateApiKey(request.getConnectorId(), newApiKeyHash);

            if (!success) {
                throw Status.NOT_FOUND
                    .withDescription("Connector not found: " + request.getConnectorId())
                    .asRuntimeException();
            }

            // TODO: Handle invalidate_old_immediately flag and grace period

            LOG.infof("Rotated API key for connector %s", request.getConnectorId());

            return RotateApiKeyResponse.newBuilder()
                .setSuccess(true)
                .setNewApiKey(newApiKey)  // Return plaintext key ONCE
                .setMessage("API key rotated successfully")
                // TODO: Set old_key_expires based on grace period
                .build();
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    /**
     * Updates connector properties and metadata.
     *
     * Reactive semantics:
     * - Cold `Uni` running on the default worker pool for repository reads/writes and JSON (de)serialization.
     * - Merges provided metadata fields with the existing stored JSON; unspecified fields are preserved.
     *
     * Side effects:
     * - Writes to the connectors table; updates `connector_name`, `metadata`, and `updated_at`.
     * - Emits INFO logs for auditing.
     *
     * @param request Update parameters including `connector_id`, optional `connector_name`, and metadata fields (`s3_bucket`, `s3_base_path`, `max_file_size`, `rate_limit_per_minute`, `default_metadata`).
     * @return `Uni` emitting `UpdateConnectorResponse` including the updated `ConnectorRegistration`.
     * @throws IllegalArgumentException when `connector_id` is missing or empty.
     * @throws io.grpc.StatusRuntimeException NOT_FOUND when the connector does not exist.
     *
     * Usage example:
     * ```java
     * service.updateConnector(UpdateConnectorRequest.newBuilder()
     *     .setConnectorId("abc-123")
     *     .setConnectorName("Docs S3")
     *     .setS3Bucket("company-docs")
     *     .putDefaultMetadata("source","s3")
     *     .build());
     * ```
     */
    @Override
    public Uni<UpdateConnectorResponse> updateConnector(UpdateConnectorRequest request) {
        return Uni.createFrom().item(() -> {
            LOG.infof("Updating connector: %s", request.getConnectorId());

            if (request.getConnectorId() == null || request.getConnectorId().isEmpty()) {
                throw new IllegalArgumentException("Connector ID is required");
            }

            // Build updated metadata if S3 config provided
            String metadataJson = null;
            if (request.getS3Bucket() != null || request.getS3BasePath() != null ||
                request.getMaxFileSize() > 0 || request.getRateLimitPerMinute() > 0 ||
                !request.getDefaultMetadataMap().isEmpty()) {

                // Merge with existing metadata
                Connector existing = connectorRepository.findByConnectorId(request.getConnectorId());
                if (existing != null) {
                    ConnectorMetadata metadata = ConnectorMetadata.fromJson(existing.metadata);

                    // Update fields if provided
                    if (request.getS3Bucket() != null && !request.getS3Bucket().isEmpty()) {
                        metadata.setS3Bucket(request.getS3Bucket());
                    }
                    if (request.getS3BasePath() != null && !request.getS3BasePath().isEmpty()) {
                        metadata.setS3BasePath(request.getS3BasePath());
                    }
                    if (request.getMaxFileSize() > 0) {
                        metadata.setMaxFileSize(request.getMaxFileSize());
                    }
                    if (request.getRateLimitPerMinute() > 0) {
                        metadata.setRateLimitPerMinute(request.getRateLimitPerMinute());
                    }
                    if (!request.getDefaultMetadataMap().isEmpty()) {
                        metadata.getDefaultMetadata().putAll(request.getDefaultMetadataMap());
                    }

                    metadataJson = metadata.toJson();
                }
            }

            // Use transactional repository method
            Connector updated = connectorRepository.updateConnector(
                request.getConnectorId(),
                request.getConnectorName(),
                metadataJson
            );

            if (updated == null) {
                throw Status.NOT_FOUND
                    .withDescription("Connector not found: " + request.getConnectorId())
                    .asRuntimeException();
            }

            return UpdateConnectorResponse.newBuilder()
                .setSuccess(true)
                .setMessage("Connector updated successfully")
                .setConnector(toProtoConnectorRegistration(updated))
                .build();
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    /**
     * Returns crawl history entries for a connector.
     *
     * Reactive semantics:
     * - Cold `Uni` that will, when implemented, perform repository reads on the worker pool.
     * - Current implementation fails immediately with `UNIMPLEMENTED`.
     *
     * Side effects:
     * - None at present.
     *
     * @param request Request containing `connector_id` and optional pagination/filter fields.
     * @return `Uni` expected to emit a `GetCrawlHistoryResponse` once implemented; currently fails with `UNIMPLEMENTED` status.
     */
    public Uni<ai.pipestream.connector.intake.v1.GetCrawlHistoryResponse> getCrawlHistory(
            ai.pipestream.connector.intake.v1.GetCrawlHistoryRequest request) {
        // TODO: Implement crawl history tracking
        return Uni.createFrom().failure(
            Status.UNIMPLEMENTED
                .withDescription("GetCrawlHistory not yet implemented")
                .asRuntimeException()
        );
    }

    /**
     * Convert Connector entity to ConnectorRegistration proto.
     */
    private ConnectorRegistration toProtoConnectorRegistration(Connector connector) {
        // Get linked accounts
        List<String> accountIds = connectorRepository.getLinkedAccounts(connector.connectorId);
        String primaryAccountId = accountIds.isEmpty() ? "" : accountIds.get(0);

        // Explicitly set active to ensure false values are serialized
        boolean isActive = connector.active != null ? connector.active : false;
        LOG.debugf("Converting connector %s: active field=%s, isActive=%s",
            connector.connectorId, connector.active, isActive);

        // Parse metadata JSON
        ConnectorMetadata metadata = ConnectorMetadata.fromJson(connector.metadata);

        return ConnectorRegistration.newBuilder()
            .setConnectorId(connector.connectorId)
            .setConnectorName(connector.connectorName)
            .setConnectorType(connector.connectorType)
            .setAccountId(primaryAccountId)  // Primary account (first linked)
            // Note: api_key is never returned (it's hashed)
            .setApiKey("")  // Never expose even the hash
            .setS3Bucket(metadata.getS3Bucket())
            .setS3BasePath(metadata.getS3BasePath())
            .setMaxFileSize(metadata.getMaxFileSize())
            .setRateLimitPerMinute(metadata.getRateLimitPerMinute())
            .putAllDefaultMetadata(metadata.getDefaultMetadata())
            .setActive(isActive)  // Always explicitly set, even if false
            .setCreated(Timestamp.newBuilder()
                .setSeconds(connector.createdAt.toEpochSecond())
                .setNanos(connector.createdAt.getNano())
                .build())
            .setUpdated(Timestamp.newBuilder()
                .setSeconds(connector.updatedAt.toEpochSecond())
                .setNanos(connector.updatedAt.getNano())
                .build())
            .build();
    }
}
