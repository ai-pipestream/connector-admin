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
        // Validation
        if (request.getConnectorId() == null || request.getConnectorId().isBlank()) {
            return Uni.createFrom().failure(new IllegalArgumentException("connector_id is required"));
        }
        if (request.getSchemaVersion() == null || request.getSchemaVersion().isBlank()) {
            return Uni.createFrom().failure(new IllegalArgumentException("schema_version is required"));
        }
        if (!request.hasCustomConfigSchema() || request.getCustomConfigSchema().getFieldsCount() == 0) {
            return Uni.createFrom().failure(new IllegalArgumentException("custom_config_schema is required"));
        }
        if (!request.hasNodeCustomConfigSchema() || request.getNodeCustomConfigSchema().getFieldsCount() == 0) {
            return Uni.createFrom().failure(new IllegalArgumentException("node_custom_config_schema is required"));
        }

        String customSchemaJson = structToJson(request.getCustomConfigSchema());
        String nodeSchemaJson = structToJson(request.getNodeCustomConfigSchema());

        // Reactive composition: verify connector exists, then check for duplicate, then create
        return connectorRegistrationRepository.findConnectorById(request.getConnectorId())
            .flatMap(connector -> {
                if (connector == null) {
                    return Uni.createFrom().failure(Status.NOT_FOUND
                        .withDescription("Connector type not found: " + request.getConnectorId())
                        .asRuntimeException());
                }
                return connectorRegistrationRepository.findSchemaByConnectorAndVersion(
                    request.getConnectorId(), request.getSchemaVersion());
            })
            .flatMap(existing -> {
                if (existing != null) {
                    return Uni.createFrom().failure(Status.ALREADY_EXISTS
                        .withDescription("Schema already exists for connector " + request.getConnectorId()
                            + " version " + request.getSchemaVersion())
                        .asRuntimeException());
                }
                return connectorRegistrationRepository.createSchema(
                    request.getSchemaId(),
                    request.getConnectorId(),
                    request.getSchemaVersion(),
                    customSchemaJson,
                    nodeSchemaJson,
                    request.getCreatedBy());
            })
            .map(created -> CreateConnectorConfigSchemaResponse.newBuilder()
                .setSuccess(true)
                .setMessage("Schema created successfully")
                .setSchema(toProtoSchema(created))
                .build());
    }

    @Override
    public Uni<GetConnectorConfigSchemaResponse> getConnectorConfigSchema(GetConnectorConfigSchemaRequest request) {
        if (request.getSchemaId() == null || request.getSchemaId().isBlank()) {
            return Uni.createFrom().failure(new IllegalArgumentException("schema_id is required"));
        }

        return connectorRegistrationRepository.findSchemaById(request.getSchemaId())
            .flatMap(schema -> {
                if (schema == null) {
                    return Uni.createFrom().failure(Status.NOT_FOUND
                        .withDescription("Schema not found: " + request.getSchemaId())
                        .asRuntimeException());
                }
                return Uni.createFrom().item(GetConnectorConfigSchemaResponse.newBuilder()
                    .setSchema(toProtoSchema(schema))
                    .build());
            });
    }

    @Override
    public Uni<ListConnectorConfigSchemasResponse> listConnectorConfigSchemas(ListConnectorConfigSchemasRequest request) {
        if (request.getConnectorId() == null || request.getConnectorId().isBlank()) {
            return Uni.createFrom().failure(new IllegalArgumentException("connector_id is required"));
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

        // Capture offset and pageSize in effectively final variables for lambda
        final int finalOffset = offset;
        final int finalPageSize = pageSize;

        // Fetch schemas and count in parallel
        Uni<List<ConnectorConfigSchema>> schemasUni = connectorRegistrationRepository.listSchemas(
            request.getConnectorId(), finalPageSize + 1, finalOffset);
        Uni<Long> countUni = connectorRegistrationRepository.countSchemas(request.getConnectorId());

        return Uni.combine().all().unis(schemasUni, countUni).asTuple().map(tuple -> {
            List<ConnectorConfigSchema> schemas = tuple.getItem1();
            Long total = tuple.getItem2();
            String nextPageToken = "";
            List<ConnectorConfigSchema> finalSchemas = schemas;
            if (schemas.size() > finalPageSize) {
                finalSchemas = schemas.subList(0, finalPageSize);
                nextPageToken = String.valueOf(finalOffset + finalPageSize);
            }

            ListConnectorConfigSchemasResponse.Builder builder = ListConnectorConfigSchemasResponse.newBuilder()
                .setTotalCount((int) Math.min(total, Integer.MAX_VALUE));
            if (!nextPageToken.isEmpty()) {
                builder.setNextPageToken(nextPageToken);
            }
            for (ConnectorConfigSchema schema : finalSchemas) {
                builder.addSchemas(toProtoSchema(schema));
            }
            return builder.build();
        });
    }

    @Override
    public Uni<DeleteConnectorConfigSchemaResponse> deleteConnectorConfigSchema(DeleteConnectorConfigSchemaRequest request) {
        if (request.getSchemaId() == null || request.getSchemaId().isBlank()) {
            return Uni.createFrom().failure(new IllegalArgumentException("schema_id is required"));
        }

        // Check references in parallel
        Uni<Boolean> referencedByConnector = connectorRegistrationRepository.isSchemaReferencedByConnector(request.getSchemaId());
        Uni<Boolean> referencedByDataSource = connectorRegistrationRepository.isSchemaReferencedByAnyDataSource(request.getSchemaId());

        return Uni.combine().all().unis(referencedByConnector, referencedByDataSource).asTuple().map(tuple -> {
            Boolean byConnector = tuple.getItem1();
            Boolean byDataSource = tuple.getItem2();
            if (byConnector || byDataSource) {
                throw Status.FAILED_PRECONDITION
                    .withDescription("Schema is still referenced by a Connector or DataSource: " + request.getSchemaId())
                    .asRuntimeException();
            }
            return null;
        })
        .flatMap(v -> connectorRegistrationRepository.deleteSchema(request.getSchemaId()))
        .flatMap(deleted -> {
            if (!deleted) {
                return Uni.createFrom().failure(Status.NOT_FOUND
                    .withDescription("Schema not found: " + request.getSchemaId())
                    .asRuntimeException());
            }
            return Uni.createFrom().item(DeleteConnectorConfigSchemaResponse.newBuilder()
                .setSuccess(true)
                .setMessage("Schema deleted successfully")
                .build());
        });
    }

    @Override
    public Uni<SetConnectorCustomConfigSchemaResponse> setConnectorCustomConfigSchema(SetConnectorCustomConfigSchemaRequest request) {
        if (request.getConnectorId() == null || request.getConnectorId().isBlank()) {
            return Uni.createFrom().failure(new IllegalArgumentException("connector_id is required"));
        }

        // If schema ID provided, verify it exists first
        Uni<Void> schemaCheck = Uni.createFrom().voidItem();
        if (request.getSchemaId() != null && !request.getSchemaId().isBlank()) {
            schemaCheck = connectorRegistrationRepository.findSchemaById(request.getSchemaId())
                .flatMap(schema -> {
                    if (schema == null) {
                        return Uni.createFrom().failure(Status.NOT_FOUND
                            .withDescription("Schema not found: " + request.getSchemaId())
                            .asRuntimeException());
                    }
                    return Uni.createFrom().voidItem();
                });
        }

        return schemaCheck
            .flatMap(v -> connectorRegistrationRepository.setConnectorCustomConfigSchema(
                request.getConnectorId(), request.getSchemaId()))
            .flatMap(updated -> {
                if (updated == null) {
                    return Uni.createFrom().failure(Status.NOT_FOUND
                        .withDescription("Connector type not found: " + request.getConnectorId())
                        .asRuntimeException());
                }
                return Uni.createFrom().item(SetConnectorCustomConfigSchemaResponse.newBuilder()
                    .setSuccess(true)
                    .setMessage("Connector schema reference updated successfully")
                    .setConnector(toProtoConnector(updated))
                    .build());
            });
    }

    @Override
    public Uni<UpdateConnectorTypeDefaultsResponse> updateConnectorTypeDefaults(UpdateConnectorTypeDefaultsRequest request) {
        if (request.getConnectorId() == null || request.getConnectorId().isBlank()) {
            return Uni.createFrom().failure(new IllegalArgumentException("connector_id is required"));
        }

        Boolean persistOrNull = request.hasDefaultPersistPipedoc() ? request.getDefaultPersistPipedoc() : null;
        Integer maxInlineOrNull = request.hasDefaultMaxInlineSizeBytes() ? request.getDefaultMaxInlineSizeBytes() : null;

        String defaultCustomConfigJson = null;
        if (request.hasDefaultCustomConfig() && request.getDefaultCustomConfig().getFieldsCount() > 0) {
            defaultCustomConfigJson = structToJson(request.getDefaultCustomConfig());
        }

        List<String> tags = null;
        if (request.getTagsCount() > 0) {
            tags = request.getTagsList();
        }

        return connectorRegistrationRepository.updateConnectorDefaults(
                request.getConnectorId(),
                persistOrNull,
                maxInlineOrNull,
                defaultCustomConfigJson,
                request.getDisplayName(),
                request.getOwner(),
                request.getDocumentationUrl(),
                tags)
            .flatMap(updated -> {
                if (updated == null) {
                    return Uni.createFrom().failure(Status.NOT_FOUND
                        .withDescription("Connector type not found: " + request.getConnectorId())
                        .asRuntimeException());
                }
                return Uni.createFrom().item(UpdateConnectorTypeDefaultsResponse.newBuilder()
                    .setSuccess(true)
                    .setMessage("Connector defaults updated successfully")
                    .setConnector(toProtoConnector(updated))
                    .build());
            });
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

    private ai.pipestream.connector.v1.Connector toProtoConnector(Connector c) {
        // Reuse DataSourceAdminServiceImpl's mapping behavior by matching fields directly.
        ai.pipestream.connector.v1.ManagementType mgmtType = 
            ai.pipestream.connector.v1.ManagementType.MANAGEMENT_TYPE_UNSPECIFIED;
        if (c.managementType != null) {
            if ("MANAGED".equalsIgnoreCase(c.managementType)) {
                mgmtType = ai.pipestream.connector.v1.ManagementType.MANAGEMENT_TYPE_MANAGED;
            } else if ("UNMANAGED".equalsIgnoreCase(c.managementType)) {
                mgmtType = ai.pipestream.connector.v1.ManagementType.MANAGEMENT_TYPE_UNMANAGED;
            }
        }
        
        ai.pipestream.connector.v1.Connector.Builder builder =
            ai.pipestream.connector.v1.Connector.newBuilder()
                .setConnectorId(c.connectorId)
                .setConnectorType(c.connectorType)
                .setName(c.name)
                .setManagementType(mgmtType);

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
        if (c.tags != null && !c.tags.isEmpty()) {
            builder.addAllTags(c.tags);
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