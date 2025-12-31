package ai.pipestream.connector.service;

import ai.pipestream.connector.entity.Connector;
import ai.pipestream.connector.entity.ConnectorConfigSchema;
import ai.pipestream.connector.intake.v1.CreateConnectorConfigSchemaRequest;
import ai.pipestream.connector.intake.v1.CreateConnectorConfigSchemaResponse;
import ai.pipestream.connector.intake.v1.DeleteConnectorConfigSchemaRequest;
import ai.pipestream.connector.intake.v1.DeleteConnectorConfigSchemaResponse;
import ai.pipestream.connector.intake.v1.GetConnectorConfigSchemaRequest;
import ai.pipestream.connector.intake.v1.GetConnectorConfigSchemaResponse;
import ai.pipestream.connector.intake.v1.ListConnectorConfigSchemasRequest;
import ai.pipestream.connector.intake.v1.ListConnectorConfigSchemasResponse;
import ai.pipestream.connector.intake.v1.MutinyConnectorRegistrationServiceGrpc;
import ai.pipestream.connector.intake.v1.SetConnectorCustomConfigSchemaRequest;
import ai.pipestream.connector.intake.v1.SetConnectorCustomConfigSchemaResponse;
import ai.pipestream.connector.intake.v1.UpdateConnectorTypeDefaultsRequest;
import ai.pipestream.connector.intake.v1.UpdateConnectorTypeDefaultsResponse;
import ai.pipestream.connector.repository.ConnectorRegistrationRepository;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Status;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.OffsetDateTime;
import java.util.List;

/**
 * gRPC service implementation for connector registration and schema CRUD.
 * <p>
 * Proto Definition: intake/proto/ai/pipestream/connector/intake/v1/connector_registration.proto
 */
@GrpcService
public class ConnectorRegistrationServiceImpl extends MutinyConnectorRegistrationServiceGrpc.ConnectorRegistrationServiceImplBase {

    private static final Logger LOG = Logger.getLogger(ConnectorRegistrationServiceImpl.class);

    @Inject
    ConnectorRegistrationRepository connectorRegistrationRepository;

    @Override
    public Uni<CreateConnectorConfigSchemaResponse> createConnectorConfigSchema(CreateConnectorConfigSchemaRequest request) {
        return Uni.createFrom().item(() -> {
            if (request.getConnectorId() == null || request.getConnectorId().isBlank()) {
                throw new IllegalArgumentException("connector_id is required");
            }
            if (request.getSchemaVersion() == null || request.getSchemaVersion().isBlank()) {
                throw new IllegalArgumentException("schema_version is required");
            }
            if (!request.hasCustomConfigSchema() || request.getCustomConfigSchema().getFieldsCount() == 0) {
                throw new IllegalArgumentException("custom_config_schema is required");
            }
            if (!request.hasNodeCustomConfigSchema() || request.getNodeCustomConfigSchema().getFieldsCount() == 0) {
                throw new IllegalArgumentException("node_custom_config_schema is required");
            }

            Connector connector = connectorRegistrationRepository.findConnectorById(request.getConnectorId());
            if (connector == null) {
                throw Status.NOT_FOUND
                    .withDescription("Connector type not found: " + request.getConnectorId())
                    .asRuntimeException();
            }

            ConnectorConfigSchema existing = connectorRegistrationRepository.findSchemaByConnectorAndVersion(
                request.getConnectorId(), request.getSchemaVersion());
            if (existing != null) {
                throw Status.ALREADY_EXISTS
                    .withDescription("Schema already exists for connector " + request.getConnectorId()
                        + " version " + request.getSchemaVersion())
                    .asRuntimeException();
            }

            String customSchemaJson = structToJson(request.getCustomConfigSchema());
            String nodeSchemaJson = structToJson(request.getNodeCustomConfigSchema());

            ConnectorConfigSchema created = connectorRegistrationRepository.createSchema(
                request.getSchemaId(),
                request.getConnectorId(),
                request.getSchemaVersion(),
                customSchemaJson,
                nodeSchemaJson,
                request.getCreatedBy()
            );

            return CreateConnectorConfigSchemaResponse.newBuilder()
                .setSuccess(true)
                .setMessage("Schema created successfully")
                .setSchema(toProtoSchema(created))
                .build();
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    @Override
    public Uni<GetConnectorConfigSchemaResponse> getConnectorConfigSchema(GetConnectorConfigSchemaRequest request) {
        return Uni.createFrom().item(() -> {
            if (request.getSchemaId() == null || request.getSchemaId().isBlank()) {
                throw new IllegalArgumentException("schema_id is required");
            }

            ConnectorConfigSchema schema = connectorRegistrationRepository.findSchemaById(request.getSchemaId());
            if (schema == null) {
                throw Status.NOT_FOUND
                    .withDescription("Schema not found: " + request.getSchemaId())
                    .asRuntimeException();
            }

            return GetConnectorConfigSchemaResponse.newBuilder()
                .setSchema(toProtoSchema(schema))
                .build();
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    @Override
    public Uni<ListConnectorConfigSchemasResponse> listConnectorConfigSchemas(ListConnectorConfigSchemasRequest request) {
        return Uni.createFrom().item(() -> {
            if (request.getConnectorId() == null || request.getConnectorId().isBlank()) {
                throw new IllegalArgumentException("connector_id is required");
            }

            int offset = 0;
            if (request.getPageToken() != null && !request.getPageToken().isEmpty()) {
                try {
                    offset = Integer.parseInt(request.getPageToken());
                } catch (NumberFormatException e) {
                    LOG.warnf("Invalid page token '%s', using offset 0", request.getPageToken());
                }
            }
            int pageSize = request.getPageSize() > 0 ? request.getPageSize() : 50;

            List<ConnectorConfigSchema> schemas = connectorRegistrationRepository.listSchemas(
                request.getConnectorId(), pageSize + 1, offset);

            String nextPageToken = "";
            if (schemas.size() > pageSize) {
                schemas = schemas.subList(0, pageSize);
                nextPageToken = String.valueOf(offset + pageSize);
            }

            long total = connectorRegistrationRepository.countSchemas(request.getConnectorId());

            ListConnectorConfigSchemasResponse.Builder builder = ListConnectorConfigSchemasResponse.newBuilder()
                .setTotalCount((int) Math.min(total, Integer.MAX_VALUE));
            if (!nextPageToken.isEmpty()) {
                builder.setNextPageToken(nextPageToken);
            }
            for (ConnectorConfigSchema schema : schemas) {
                builder.addSchemas(toProtoSchema(schema));
            }
            return builder.build();
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    @Override
    public Uni<DeleteConnectorConfigSchemaResponse> deleteConnectorConfigSchema(DeleteConnectorConfigSchemaRequest request) {
        return Uni.createFrom().item(() -> {
            if (request.getSchemaId() == null || request.getSchemaId().isBlank()) {
                throw new IllegalArgumentException("schema_id is required");
            }

            if (connectorRegistrationRepository.isSchemaReferencedByConnector(request.getSchemaId())
                || connectorRegistrationRepository.isSchemaReferencedByAnyDataSource(request.getSchemaId())) {
                throw Status.FAILED_PRECONDITION
                    .withDescription("Schema is still referenced by a Connector or DataSource: " + request.getSchemaId())
                    .asRuntimeException();
            }

            boolean deleted = connectorRegistrationRepository.deleteSchema(request.getSchemaId());
            if (!deleted) {
                throw Status.NOT_FOUND
                    .withDescription("Schema not found: " + request.getSchemaId())
                    .asRuntimeException();
            }

            return DeleteConnectorConfigSchemaResponse.newBuilder()
                .setSuccess(true)
                .setMessage("Schema deleted successfully")
                .build();
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    @Override
    public Uni<SetConnectorCustomConfigSchemaResponse> setConnectorCustomConfigSchema(SetConnectorCustomConfigSchemaRequest request) {
        return Uni.createFrom().item(() -> {
            if (request.getConnectorId() == null || request.getConnectorId().isBlank()) {
                throw new IllegalArgumentException("connector_id is required");
            }

            if (request.getSchemaId() != null && !request.getSchemaId().isBlank()) {
                ConnectorConfigSchema schema = connectorRegistrationRepository.findSchemaById(request.getSchemaId());
                if (schema == null) {
                    throw Status.NOT_FOUND
                        .withDescription("Schema not found: " + request.getSchemaId())
                        .asRuntimeException();
                }
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
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    @Override
    public Uni<UpdateConnectorTypeDefaultsResponse> updateConnectorTypeDefaults(UpdateConnectorTypeDefaultsRequest request) {
        return Uni.createFrom().item(() -> {
            if (request.getConnectorId() == null || request.getConnectorId().isBlank()) {
                throw new IllegalArgumentException("connector_id is required");
            }

            Boolean persistOrNull = request.hasDefaultPersistPipedoc() ? request.getDefaultPersistPipedoc() : null;
            Integer maxInlineOrNull = request.hasDefaultMaxInlineSizeBytes() ? request.getDefaultMaxInlineSizeBytes() : null;

            String defaultCustomConfigJson = null;
            if (request.hasDefaultCustomConfig() && request.getDefaultCustomConfig().getFieldsCount() > 0) {
                defaultCustomConfigJson = structToJson(request.getDefaultCustomConfig());
            }

            String[] tags = null;
            if (request.getTagsCount() > 0) {
                tags = request.getTagsList().toArray(new String[0]);
            }

            Connector updated = connectorRegistrationRepository.updateConnectorDefaults(
                request.getConnectorId(),
                persistOrNull,
                maxInlineOrNull,
                defaultCustomConfigJson,
                request.getDisplayName(),
                request.getOwner(),
                request.getDocumentationUrl(),
                tags
            );

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
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    // ========================================================================
    // Proto mapping helpers
    // ========================================================================

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
        if (schema.createdAt != null) {
            builder.setCreatedAt(toTimestamp(schema.createdAt));
        }
        if (schema.createdBy != null) {
            builder.setCreatedBy(schema.createdBy);
        }
        if (schema.apicurioArtifactId != null) {
            builder.setApicurioArtifactId(schema.apicurioArtifactId);
        }
        if (schema.apicurioGlobalId != null) {
            builder.setApicurioGlobalId(schema.apicurioGlobalId);
        }
        if (schema.lastSyncAttempt != null) {
            builder.setLastSyncAttempt(toTimestamp(schema.lastSyncAttempt));
        }
        if (schema.syncError != null) {
            builder.setSyncError(schema.syncError);
        }
        return builder.build();
    }

    private ai.pipestream.connector.intake.v1.Connector toProtoConnector(Connector c) {
        // Reuse DataSourceAdminServiceImpl's mapping behavior by matching fields directly.
        ai.pipestream.connector.intake.v1.Connector.Builder builder =
            ai.pipestream.connector.intake.v1.Connector.newBuilder()
                .setConnectorId(c.connectorId)
                .setConnectorType(c.connectorType)
                .setName(c.name)
                .setManagementType("MANAGED".equalsIgnoreCase(c.managementType)
                    ? ai.pipestream.connector.intake.v1.ManagementType.MANAGEMENT_TYPE_MANAGED
                    : ai.pipestream.connector.intake.v1.ManagementType.MANAGEMENT_TYPE_UNMANAGED);

        if (c.description != null) {
            builder.setDescription(c.description);
        }
        if (c.createdAt != null) {
            builder.setCreatedAt(toTimestamp(c.createdAt));
        }
        if (c.updatedAt != null) {
            builder.setUpdatedAt(toTimestamp(c.updatedAt));
        }
        if (c.customConfigSchemaId != null) {
            builder.setCustomConfigSchemaId(c.customConfigSchemaId);
        }
        if (c.defaultPersistPipedoc != null) {
            builder.setDefaultPersistPipedoc(c.defaultPersistPipedoc);
        }
        if (c.defaultMaxInlineSizeBytes != null) {
            builder.setDefaultMaxInlineSizeBytes(c.defaultMaxInlineSizeBytes);
        }
        if (c.defaultCustomConfig != null && !c.defaultCustomConfig.isBlank() && !c.defaultCustomConfig.equals("{}")) {
            builder.setDefaultCustomConfig(jsonToStruct(c.defaultCustomConfig));
        }
        if (c.displayName != null) {
            builder.setDisplayName(c.displayName);
        }
        if (c.owner != null) {
            builder.setOwner(c.owner);
        }
        if (c.documentationUrl != null) {
            builder.setDocumentationUrl(c.documentationUrl);
        }
        if (c.tags != null && c.tags.length > 0) {
            builder.addAllTags(List.of(c.tags));
        }

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
        } catch (Exception e) {
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
        } catch (Exception e) {
            throw Status.INTERNAL
                .withDescription("Failed to parse JSON into protobuf Struct")
                .withCause(e)
                .asRuntimeException();
        }
    }
}


