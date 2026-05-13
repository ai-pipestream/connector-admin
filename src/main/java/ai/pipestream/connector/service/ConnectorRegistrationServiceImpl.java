package ai.pipestream.connector.service;

import ai.pipestream.connector.entity.Connector;
import ai.pipestream.connector.entity.ConnectorConfigSchema;
import ai.pipestream.connector.intake.v1.ConnectorRegistrationServiceGrpc;
import ai.pipestream.connector.intake.v1.CreateConnectorConfigSchemaRequest;
import ai.pipestream.connector.intake.v1.CreateConnectorConfigSchemaResponse;
import ai.pipestream.connector.intake.v1.CreateConnectorTypeRequest;
import ai.pipestream.connector.intake.v1.CreateConnectorTypeResponse;
import ai.pipestream.connector.intake.v1.DeleteConnectorConfigSchemaRequest;
import ai.pipestream.connector.intake.v1.DeleteConnectorConfigSchemaResponse;
import ai.pipestream.connector.intake.v1.DeleteConnectorTypeRequest;
import ai.pipestream.connector.intake.v1.DeleteConnectorTypeResponse;
import ai.pipestream.connector.intake.v1.GetConnectorConfigSchemaRequest;
import ai.pipestream.connector.intake.v1.GetConnectorConfigSchemaResponse;
import ai.pipestream.connector.intake.v1.ListConnectorConfigSchemasRequest;
import ai.pipestream.connector.intake.v1.ListConnectorConfigSchemasResponse;
import ai.pipestream.connector.intake.v1.SetConnectorCustomConfigSchemaRequest;
import ai.pipestream.connector.intake.v1.SetConnectorCustomConfigSchemaResponse;
import ai.pipestream.connector.intake.v1.UpdateConnectorTypeDefaultsRequest;
import ai.pipestream.connector.intake.v1.UpdateConnectorTypeDefaultsResponse;
import ai.pipestream.connector.repository.ConnectorRegistrationRepository;
import ai.pipestream.connector.v1.ManagementType;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.quarkus.grpc.GrpcService;
import io.smallrye.common.annotation.Blocking;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.OffsetDateTime;
import java.util.List;

/**
 * Production gRPC service for connector type registration and schema lifecycle.
 *
 * <p>Connector types are the catalog entries that datasource records bind to.
 * They define connector identity, ownership metadata, default behavior, and the
 * optional JSON Schemas used to validate connector-specific custom configuration.
 *
 * <p>This service enforces referential integrity at the API boundary. Connector
 * types and schema versions cannot be removed while datasources or catalog
 * records still reference them, so operational cleanup cannot accidentally break
 * an active ingestion path.
 *
 * <p>The service uses standard grpc-java {@link StreamObserver} callbacks and
 * blocking repository calls. Generated Mutiny stubs may still exist for client
 * compatibility, but hand-written production code is intentionally imperative.
 */
@GrpcService
@Blocking
public class ConnectorRegistrationServiceImpl extends ConnectorRegistrationServiceGrpc.ConnectorRegistrationServiceImplBase {

    private static final Logger LOG = Logger.getLogger(ConnectorRegistrationServiceImpl.class);

    /**
     * Default constructor for gRPC service instantiation.
     */
    public ConnectorRegistrationServiceImpl() {}

    @Inject
    ConnectorRegistrationRepository connectorRegistrationRepository;

    /**
     * Create a new connector type in the catalog.
     *
     * @param request The creation request containing connector metadata
     * @param observer Stream observer for the creation response
     */
    @Override
    public void createConnectorType(CreateConnectorTypeRequest request, StreamObserver<CreateConnectorTypeResponse> observer) {
        respond(observer, () -> {
            if (request.getConnectorType() == null || request.getConnectorType().isBlank()) {
                throw Status.INVALID_ARGUMENT.withDescription("connector_type is required").asRuntimeException();
            }
            if (request.getName() == null || request.getName().isBlank()) {
                throw Status.INVALID_ARGUMENT.withDescription("name is required").asRuntimeException();
            }

            String connectorType = request.getConnectorType().trim().toLowerCase();
            if (connectorRegistrationRepository.findConnectorByType(connectorType) != null) {
                throw Status.ALREADY_EXISTS
                    .withDescription("Connector type already exists: " + connectorType)
                    .asRuntimeException();
            }

            String managementType = request.getManagementType() == ManagementType.MANAGEMENT_TYPE_MANAGED
                ? "MANAGED"
                : "UNMANAGED";
            try {
                Connector created = connectorRegistrationRepository.createConnector(
                    connectorType,
                    request.getName(),
                    request.getDescription(),
                    managementType,
                    request.getDisplayName(),
                    request.getOwner(),
                    request.getDocumentationUrl(),
                    request.getTagsList().isEmpty() ? null : request.getTagsList());
                return CreateConnectorTypeResponse.newBuilder()
                    .setSuccess(true)
                    .setMessage("Connector type '" + connectorType + "' created with id " + created.connectorId)
                    .setConnector(toProtoConnector(created))
                    .build();
            } catch (org.hibernate.exception.ConstraintViolationException e) {
                throw Status.ALREADY_EXISTS
                    .withDescription("Connector type already exists: " + connectorType)
                    .asRuntimeException();
            }
        });
    }

    /**
     * Delete an existing connector type if not referenced by active datasources.
     *
     * @param request The deletion request containing the connector ID
     * @param observer Stream observer for the deletion response
     */
    @Override
    public void deleteConnectorType(DeleteConnectorTypeRequest request, StreamObserver<DeleteConnectorTypeResponse> observer) {
        respond(observer, () -> {
            if (request.getConnectorId() == null || request.getConnectorId().isBlank()) {
                throw Status.INVALID_ARGUMENT.withDescription("connector_id is required").asRuntimeException();
            }
            if (connectorRegistrationRepository.hasDataSources(request.getConnectorId())) {
                throw Status.FAILED_PRECONDITION
                    .withDescription("Cannot delete connector type: active DataSources reference it")
                    .asRuntimeException();
            }
            if (connectorRegistrationRepository.hasSchemas(request.getConnectorId())) {
                throw Status.FAILED_PRECONDITION
                    .withDescription("Cannot delete connector type: config schemas still reference it")
                    .asRuntimeException();
            }
            if (!connectorRegistrationRepository.deleteConnector(request.getConnectorId())) {
                throw Status.NOT_FOUND
                    .withDescription("Connector type not found: " + request.getConnectorId())
                    .asRuntimeException();
            }
            return DeleteConnectorTypeResponse.newBuilder()
                .setSuccess(true)
                .setMessage("Connector type deleted")
                .build();
        });
    }

    /**
     * Register a new configuration schema for a connector type.
     *
     * @param request The schema creation request
     * @param observer Stream observer for the creation response
     */
    @Override
    public void createConnectorConfigSchema(CreateConnectorConfigSchemaRequest request,
                                            StreamObserver<CreateConnectorConfigSchemaResponse> observer) {
        respond(observer, () -> {
            if (request.getConnectorId() == null || request.getConnectorId().isBlank()) {
                throw Status.INVALID_ARGUMENT.withDescription("connector_id is required").asRuntimeException();
            }
            if (request.getSchemaVersion() == null || request.getSchemaVersion().isBlank()) {
                throw Status.INVALID_ARGUMENT.withDescription("schema_version is required").asRuntimeException();
            }
            if (!request.hasCustomConfigSchema() || request.getCustomConfigSchema().getFieldsCount() == 0) {
                throw Status.INVALID_ARGUMENT.withDescription("custom_config_schema is required").asRuntimeException();
            }
            if (!request.hasNodeCustomConfigSchema() || request.getNodeCustomConfigSchema().getFieldsCount() == 0) {
                throw Status.INVALID_ARGUMENT.withDescription("node_custom_config_schema is required").asRuntimeException();
            }
            if (connectorRegistrationRepository.findConnectorById(request.getConnectorId()) == null) {
                throw Status.NOT_FOUND
                    .withDescription("Connector type not found: " + request.getConnectorId())
                    .asRuntimeException();
            }
            if (connectorRegistrationRepository.findSchemaByConnectorAndVersion(
                    request.getConnectorId(), request.getSchemaVersion()) != null) {
                throw Status.ALREADY_EXISTS
                    .withDescription("Schema already exists for connector " + request.getConnectorId()
                        + " version " + request.getSchemaVersion())
                    .asRuntimeException();
            }

            ConnectorConfigSchema created = connectorRegistrationRepository.createSchema(
                request.getSchemaId(),
                request.getConnectorId(),
                request.getSchemaVersion(),
                structToJson(request.getCustomConfigSchema()),
                structToJson(request.getNodeCustomConfigSchema()),
                request.getCreatedBy());
            return CreateConnectorConfigSchemaResponse.newBuilder()
                .setSuccess(true)
                .setMessage("Schema created successfully")
                .setSchema(toProtoSchema(created))
                .build();
        });
    }

    /**
     * Retrieve a configuration schema by ID.
     *
     * @param request The retrieval request containing the schema ID
     * @param observer Stream observer for the schema response
     */
    @Override
    public void getConnectorConfigSchema(GetConnectorConfigSchemaRequest request,
                                         StreamObserver<GetConnectorConfigSchemaResponse> observer) {
        respond(observer, () -> {
            if (request.getSchemaId() == null || request.getSchemaId().isBlank()) {
                throw Status.INVALID_ARGUMENT.withDescription("schema_id is required").asRuntimeException();
            }
            ConnectorConfigSchema schema = connectorRegistrationRepository.findSchemaById(request.getSchemaId());
            if (schema == null) {
                throw Status.NOT_FOUND.withDescription("Schema not found: " + request.getSchemaId()).asRuntimeException();
            }
            return GetConnectorConfigSchemaResponse.newBuilder().setSchema(toProtoSchema(schema)).build();
        });
    }

    /**
     * List all configuration schemas for a connector type with pagination.
     *
     * @param request The listing request
     * @param observer Stream observer for the schema list response
     */
    @Override
    public void listConnectorConfigSchemas(ListConnectorConfigSchemasRequest request,
                                           StreamObserver<ListConnectorConfigSchemasResponse> observer) {
        respond(observer, () -> {
            if (request.getConnectorId() == null || request.getConnectorId().isBlank()) {
                throw Status.INVALID_ARGUMENT.withDescription("connector_id is required").asRuntimeException();
            }

            int offset = parseOffset(request.getPageToken());
            int pageSize = request.getPageSize() > 0 ? request.getPageSize() : 50;
            List<ConnectorConfigSchema> schemas = connectorRegistrationRepository.listSchemas(
                request.getConnectorId(), pageSize + 1, offset);
            long total = connectorRegistrationRepository.countSchemas(request.getConnectorId());

            String nextPageToken = "";
            List<ConnectorConfigSchema> finalSchemas = schemas;
            if (schemas.size() > pageSize) {
                finalSchemas = schemas.subList(0, pageSize);
                nextPageToken = String.valueOf(offset + pageSize);
            }

            ListConnectorConfigSchemasResponse.Builder builder = ListConnectorConfigSchemasResponse.newBuilder()
                .setTotalCount((int) Math.min(total, Integer.MAX_VALUE));
            if (!nextPageToken.isEmpty()) {
                builder.setNextPageToken(nextPageToken);
            }
            finalSchemas.forEach(schema -> builder.addSchemas(toProtoSchema(schema)));
            return builder.build();
        });
    }

    /**
     * Delete a configuration schema if not currently referenced.
     *
     * @param request The deletion request containing the schema ID
     * @param observer Stream observer for the deletion response
     */
    @Override
    public void deleteConnectorConfigSchema(DeleteConnectorConfigSchemaRequest request,
                                            StreamObserver<DeleteConnectorConfigSchemaResponse> observer) {
        respond(observer, () -> {
            if (request.getSchemaId() == null || request.getSchemaId().isBlank()) {
                throw Status.INVALID_ARGUMENT.withDescription("schema_id is required").asRuntimeException();
            }
            if (connectorRegistrationRepository.isSchemaReferencedByConnector(request.getSchemaId())
                || connectorRegistrationRepository.isSchemaReferencedByAnyDataSource(request.getSchemaId())) {
                throw Status.FAILED_PRECONDITION
                    .withDescription("Schema is still referenced by a Connector or DataSource: " + request.getSchemaId())
                    .asRuntimeException();
            }
            if (!connectorRegistrationRepository.deleteSchema(request.getSchemaId())) {
                throw Status.NOT_FOUND.withDescription("Schema not found: " + request.getSchemaId()).asRuntimeException();
            }
            return DeleteConnectorConfigSchemaResponse.newBuilder()
                .setSuccess(true)
                .setMessage("Schema deleted successfully")
                .build();
        });
    }

    /**
     * Set the active configuration schema for a connector type.
     *
     * @param request The update request
     * @param observer Stream observer for the update response
     */
    @Override
    public void setConnectorCustomConfigSchema(SetConnectorCustomConfigSchemaRequest request,
                                               StreamObserver<SetConnectorCustomConfigSchemaResponse> observer) {
        respond(observer, () -> {
            if (request.getConnectorId() == null || request.getConnectorId().isBlank()) {
                throw Status.INVALID_ARGUMENT.withDescription("connector_id is required").asRuntimeException();
            }
            if (request.getSchemaId() != null && !request.getSchemaId().isBlank()
                && connectorRegistrationRepository.findSchemaById(request.getSchemaId()) == null) {
                throw Status.NOT_FOUND.withDescription("Schema not found: " + request.getSchemaId()).asRuntimeException();
            }

            Connector updated = connectorRegistrationRepository.setConnectorCustomConfigSchema(
                request.getConnectorId(), request.getSchemaId());
            if (updated == null) {
                throw Status.NOT_FOUND
                    .withDescription("Connector type not found: " + request.getConnectorId())
                    .asRuntimeException();
            }
            return SetConnectorCustomConfigSchemaResponse.newBuilder()
                .setSuccess(true)
                .setMessage("Connector schema reference updated successfully")
                .setConnector(toProtoConnector(updated))
                .build();
        });
    }

    /**
     * Update default configuration values and metadata for a connector type.
     *
     * @param request The update request
     * @param observer Stream observer for the update response
     */
    @Override
    public void updateConnectorTypeDefaults(UpdateConnectorTypeDefaultsRequest request,
                                            StreamObserver<UpdateConnectorTypeDefaultsResponse> observer) {
        respond(observer, () -> {
            if (request.getConnectorId() == null || request.getConnectorId().isBlank()) {
                throw Status.INVALID_ARGUMENT.withDescription("connector_id is required").asRuntimeException();
            }
            String defaultCustomConfigJson = null;
            if (request.hasDefaultCustomConfig() && request.getDefaultCustomConfig().getFieldsCount() > 0) {
                defaultCustomConfigJson = structToJson(request.getDefaultCustomConfig());
            }

            Connector updated = connectorRegistrationRepository.updateConnectorDefaults(
                request.getConnectorId(),
                request.hasDefaultPersistPipedoc() ? request.getDefaultPersistPipedoc() : null,
                request.hasDefaultMaxInlineSizeBytes() ? request.getDefaultMaxInlineSizeBytes() : null,
                defaultCustomConfigJson,
                request.getDisplayName(),
                request.getOwner(),
                request.getDocumentationUrl(),
                request.getTagsCount() > 0 ? request.getTagsList() : null);
            if (updated == null) {
                throw Status.NOT_FOUND
                    .withDescription("Connector type not found: " + request.getConnectorId())
                    .asRuntimeException();
            }
            return UpdateConnectorTypeDefaultsResponse.newBuilder()
                .setSuccess(true)
                .setMessage("Connector defaults updated successfully")
                .setConnector(toProtoConnector(updated))
                .build();
        });
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

    private <T> void respond(StreamObserver<T> observer, ResponseSupplier<T> supplier) {
        try {
            observer.onNext(supplier.get());
            observer.onCompleted();
        } catch (StatusRuntimeException e) {
            observer.onError(e);
        } catch (RuntimeException e) {
            LOG.error("Unhandled connector registration gRPC failure", e);
            observer.onError(Status.UNKNOWN.withDescription(e.getMessage()).withCause(e).asRuntimeException());
        }
    }

    @FunctionalInterface
    private interface ResponseSupplier<T> {
        T get();
    }

    private ai.pipestream.connector.intake.v1.ConnectorConfigSchema toProtoSchema(ConnectorConfigSchema schema) {
        ai.pipestream.connector.intake.v1.ConnectorConfigSchema.Builder builder =
            ai.pipestream.connector.intake.v1.ConnectorConfigSchema.newBuilder()
                .setSchemaId(schema.schemaId)
                .setConnectorId(schema.connectorId)
                .setSchemaVersion(schema.schemaVersion)
                .setSyncStatus(schema.syncStatus != null ? schema.syncStatus : "");

        if (schema.customConfigSchema != null && !schema.customConfigSchema.isBlank()) {
            builder.setCustomConfigSchema(jsonToStruct(schema.customConfigSchema));
        }
        if (schema.nodeCustomConfigSchema != null && !schema.nodeCustomConfigSchema.isBlank()) {
            builder.setNodeCustomConfigSchema(jsonToStruct(schema.nodeCustomConfigSchema));
        }
        if (schema.createdAt != null) builder.setCreatedAt(toTimestamp(schema.createdAt));
        if (schema.createdBy != null) builder.setCreatedBy(schema.createdBy);
        if (schema.apicurioArtifactId != null) builder.setApicurioArtifactId(schema.apicurioArtifactId);
        if (schema.apicurioGlobalId != null) builder.setApicurioGlobalId(schema.apicurioGlobalId);
        if (schema.lastSyncAttempt != null) builder.setLastSyncAttempt(toTimestamp(schema.lastSyncAttempt));
        if (schema.syncError != null) builder.setSyncError(schema.syncError);
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
        if (c.createdAt != null) builder.setCreatedAt(toTimestamp(c.createdAt));
        if (c.updatedAt != null) builder.setUpdatedAt(toTimestamp(c.updatedAt));
        if (c.customConfigSchemaId != null) builder.setCustomConfigSchemaId(c.customConfigSchemaId);
        if (c.defaultPersistPipedoc != null) builder.setDefaultPersistPipedoc(c.defaultPersistPipedoc);
        if (c.defaultMaxInlineSizeBytes != null) builder.setDefaultMaxInlineSizeBytes(c.defaultMaxInlineSizeBytes);
        if (c.defaultCustomConfig != null && !c.defaultCustomConfig.isBlank() && !c.defaultCustomConfig.equals("{}")) {
            builder.setDefaultCustomConfig(jsonToStruct(c.defaultCustomConfig));
        }
        if (c.displayName != null) builder.setDisplayName(c.displayName);
        if (c.owner != null) builder.setOwner(c.owner);
        if (c.documentationUrl != null) builder.setDocumentationUrl(c.documentationUrl);
        if (c.tags != null && !c.tags.isEmpty()) builder.addAllTags(c.tags);
        return builder.build();
    }

    private Timestamp toTimestamp(OffsetDateTime dt) {
        return Timestamp.newBuilder()
            .setSeconds(dt.toEpochSecond())
            .setNanos(dt.getNano())
            .build();
    }

    private String structToJson(Struct struct) {
        try {
            return JsonFormat.printer().print(struct);
        } catch (InvalidProtocolBufferException e) {
            throw Status.INVALID_ARGUMENT
                .withDescription("Failed to serialize protobuf Struct to JSON")
                .withCause(e)
                .asRuntimeException();
        }
    }

    private Struct jsonToStruct(String json) {
        try {
            Struct.Builder builder = Struct.newBuilder();
            JsonFormat.parser().merge(json, builder);
            return builder.build();
        } catch (InvalidProtocolBufferException e) {
            throw Status.INTERNAL
                .withDescription("Failed to parse JSON into protobuf Struct")
                .withCause(e)
                .asRuntimeException();
        }
    }
}
