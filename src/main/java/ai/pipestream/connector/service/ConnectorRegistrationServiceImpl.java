package ai.pipestream.connector.service;

import ai.pipestream.connector.entity.Connector;
import ai.pipestream.connector.entity.ConnectorConfigSchema;
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
import ai.pipestream.connector.intake.v1.MutinyConnectorRegistrationServiceGrpc;
import ai.pipestream.connector.intake.v1.SetConnectorCustomConfigSchemaRequest;
import ai.pipestream.connector.intake.v1.SetConnectorCustomConfigSchemaResponse;
import ai.pipestream.connector.intake.v1.UpdateConnectorTypeDefaultsRequest;
import ai.pipestream.connector.intake.v1.UpdateConnectorTypeDefaultsResponse;
import ai.pipestream.connector.repository.ConnectorRegistrationRepository;
import ai.pipestream.connector.v1.ManagementType;
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
 *
 * <p>Manages the lifecycle of connector types and their JSON Schema definitions:
 * <ul>
 *   <li>Connector types — pre-seeded templates such as "s3" or "file-crawler" that
 *       accounts bind to via {@link DataSourceAdminServiceImpl}</li>
 *   <li>Config schemas — versioned JSON Schema definitions used to validate custom
 *       configuration at both the connector (Tier 1) and graph-node (Tier 2) levels</li>
 * </ul>
 *
 * <p><strong>Reactive semantics:</strong> every method returns a cold {@link io.smallrye.mutiny.Uni}
 * that performs a single database or lookup operation.  All transactional work is
 * delegated to {@link ConnectorRegistrationRepository}; this class only handles
 * protocol-level validation and proto/entity mapping.
 *
 * <p>Proto Definition:
 * {@code intake/proto/ai/pipestream/connector/intake/v1/connector_registration.proto}
 */
@GrpcService
public class ConnectorRegistrationServiceImpl extends MutinyConnectorRegistrationServiceGrpc.ConnectorRegistrationServiceImplBase {

    private static final Logger LOG = Logger.getLogger(ConnectorRegistrationServiceImpl.class);

    @Inject
    ConnectorRegistrationRepository connectorRegistrationRepository;

    /**
     * Creates a new connector type with a deterministic ID derived from connector_type.
     */
    @Override
    public Uni<CreateConnectorTypeResponse> createConnectorType(CreateConnectorTypeRequest request) {
        if (request.getConnectorType() == null || request.getConnectorType().isBlank()) {
            return Uni.createFrom().failure(Status.INVALID_ARGUMENT
                .withDescription("connector_type is required").asRuntimeException());
        }
        if (request.getName() == null || request.getName().isBlank()) {
            return Uni.createFrom().failure(Status.INVALID_ARGUMENT
                .withDescription("name is required").asRuntimeException());
        }

        String connectorType = request.getConnectorType().trim().toLowerCase();
        String managementType = "UNMANAGED";
        if (request.getManagementType() == ManagementType.MANAGEMENT_TYPE_MANAGED) {
            managementType = "MANAGED";
        }

        // Check for duplicate by type name
        String finalManagementType = managementType;
        return connectorRegistrationRepository.findConnectorByType(connectorType)
            .flatMap(existing -> {
                if (existing != null) {
                    return Uni.createFrom().failure(Status.ALREADY_EXISTS
                        .withDescription("Connector type already exists: " + connectorType)
                        .asRuntimeException());
                }
                return connectorRegistrationRepository.createConnector(
                    connectorType,
                    request.getName(),
                    request.getDescription(),
                    finalManagementType,
                    request.getDisplayName(),
                    request.getOwner(),
                    request.getDocumentationUrl(),
                    request.getTagsList().isEmpty() ? null : request.getTagsList());
            })
            .map(created -> CreateConnectorTypeResponse.newBuilder()
                .setSuccess(true)
                .setMessage("Connector type '" + connectorType + "' created with id " + created.connectorId)
                .setConnector(toProtoConnector(created))
                .build())
            .onFailure(e -> e instanceof org.hibernate.exception.ConstraintViolationException
                    || (e.getCause() != null && e.getCause() instanceof org.hibernate.exception.ConstraintViolationException))
                .recoverWithUni(e -> Uni.createFrom().failure(Status.ALREADY_EXISTS
                    .withDescription("Connector type already exists: " + connectorType)
                    .asRuntimeException()));
    }

    /**
     * Deletes a connector type. Fails if any DataSource references it.
     */
    @Override
    public Uni<DeleteConnectorTypeResponse> deleteConnectorType(DeleteConnectorTypeRequest request) {
        if (request.getConnectorId() == null || request.getConnectorId().isBlank()) {
            return Uni.createFrom().failure(Status.INVALID_ARGUMENT
                .withDescription("connector_id is required").asRuntimeException());
        }

        return connectorRegistrationRepository.hasDataSources(request.getConnectorId())
            .flatMap(hasDs -> {
                if (hasDs) {
                    return Uni.createFrom().failure(Status.FAILED_PRECONDITION
                        .withDescription("Cannot delete connector type: active DataSources reference it")
                        .asRuntimeException());
                }
                // Check if any schemas belong to this connector type
                return connectorRegistrationRepository.hasSchemas(request.getConnectorId());
            })
            .flatMap(hasSchemas -> {
                if (hasSchemas) {
                    return Uni.createFrom().<Boolean>failure(Status.FAILED_PRECONDITION
                        .withDescription("Cannot delete connector type: config schemas still reference it")
                        .asRuntimeException());
                }
                return connectorRegistrationRepository.deleteConnector(request.getConnectorId());
            })
            .flatMap(deleted -> {
                if (!deleted) {
                    return Uni.createFrom().failure(Status.NOT_FOUND
                        .withDescription("Connector type not found: " + request.getConnectorId())
                        .asRuntimeException());
                }
                return Uni.createFrom().item(DeleteConnectorTypeResponse.newBuilder()
                    .setSuccess(true)
                    .setMessage("Connector type deleted")
                    .build());
            });
    }

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

    /**
     * Lists all config schemas for a given connector type with cursor-based pagination.
     *
     * <p>The page token is an integer offset encoded as a string.  Pass the value
     * returned in {@code next_page_token} to retrieve the next page.
     *
     * @param request {@link ListConnectorConfigSchemasRequest} containing a mandatory
     *                {@code connector_id} and optional {@code page_size}/{@code page_token}
     * @return {@link ListConnectorConfigSchemasResponse} with the schema page and
     *         {@code total_count}; {@code next_page_token} is empty on the last page
     */
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

    /**
     * Deletes a config schema by ID.
     *
     * <p>Deletion is blocked when the schema is still referenced by either a
     * connector type ({@code custom_config_schema_id} FK) or any datasource
     * ({@code custom_config_schema_id} FK).  Both checks run in parallel for
     * efficiency before the delete is attempted.
     *
     * <p>Error mapping:
     * <ul>
     *   <li>Missing {@code schema_id} → {@code INVALID_ARGUMENT}</li>
     *   <li>Active reference exists → {@code FAILED_PRECONDITION}</li>
     *   <li>Schema not found → {@code NOT_FOUND}</li>
     * </ul>
     *
     * @param request {@link DeleteConnectorConfigSchemaRequest} containing the
     *                {@code schema_id} to delete
     * @return {@link DeleteConnectorConfigSchemaResponse} with {@code success=true}
     *         on successful deletion
     */
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

    /**
     * Links or unlinks a config schema to a connector type.
     *
     * <p>When {@code schema_id} is non-empty the referenced schema must already
     * exist; the connector's {@code custom_config_schema_id} is set to that value.
     * When {@code schema_id} is empty the FK is cleared (unlinking).
     *
     * <p>Error mapping:
     * <ul>
     *   <li>Missing {@code connector_id} → {@code INVALID_ARGUMENT}</li>
     *   <li>{@code schema_id} provided but not found → {@code NOT_FOUND}</li>
     *   <li>Connector type not found → {@code NOT_FOUND}</li>
     * </ul>
     *
     * @param request {@link SetConnectorCustomConfigSchemaRequest} with {@code connector_id}
     *                and optional {@code schema_id}
     * @return {@link SetConnectorCustomConfigSchemaResponse} with {@code success=true} and
     *         the updated connector proto
     */
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

    /**
     * Updates the default configuration fields of a connector type.
     *
     * <p>Only fields that are explicitly set in the request proto are applied;
     * absent/default proto fields are left unchanged in the database.  Specifically:
     * <ul>
     *   <li>{@code default_persist_pipedoc} — only updated when the wrapper field is set</li>
     *   <li>{@code default_max_inline_size_bytes} — only updated when the wrapper field is set</li>
     *   <li>{@code default_custom_config} — only updated when the Struct is non-empty</li>
     *   <li>{@code tags} — only updated when at least one tag is supplied</li>
     *   <li>{@code display_name}, {@code owner}, {@code documentation_url} — updated to the
     *       supplied value (even empty string, to allow clearing)</li>
     * </ul>
     *
     * <p>Error mapping:
     * <ul>
     *   <li>Missing {@code connector_id} → {@code INVALID_ARGUMENT}</li>
     *   <li>Connector type not found → {@code NOT_FOUND}</li>
     * </ul>
     *
     * @param request {@link UpdateConnectorTypeDefaultsRequest} with the connector ID and
     *                the subset of defaults to update
     * @return {@link UpdateConnectorTypeDefaultsResponse} with {@code success=true} and the
     *         updated connector proto
     */
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

    /**
     * Maps a {@link ConnectorConfigSchema} entity to its proto representation.
     *
     * @param schema the JPA entity to map; must not be {@code null}
     * @return the corresponding {@link ai.pipestream.connector.intake.v1.ConnectorConfigSchema} proto
     */
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

    /**
     * Maps a {@link Connector} entity to its proto representation.
     *
     * @param c the JPA entity to map; must not be {@code null}
     * @return the corresponding {@link ai.pipestream.connector.v1.Connector} proto
     */
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

    /**
     * Converts an {@link OffsetDateTime} to a protobuf {@link Timestamp}.
     *
     * @param dt the date-time to convert; must not be {@code null}
     * @return the corresponding {@link Timestamp}
     */
    private Timestamp toTimestamp(OffsetDateTime dt) {
        return Timestamp.newBuilder()
            .setSeconds(dt.toEpochSecond())
            .setNanos(dt.getNano())
            .build();
    }

    /**
     * Serializes a protobuf {@link Struct} to its JSON string representation.
     *
     * @param struct the Struct to serialize; must not be {@code null}
     * @return JSON string
     * @throws io.grpc.StatusRuntimeException with {@code INVALID_ARGUMENT} if serialization fails
     */
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

    /**
     * Parses a JSON string into a protobuf {@link Struct}.
     *
     * @param json the JSON string to parse; must not be {@code null}
     * @return the corresponding {@link Struct}
     * @throws io.grpc.StatusRuntimeException with {@code INTERNAL} if parsing fails
     */
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