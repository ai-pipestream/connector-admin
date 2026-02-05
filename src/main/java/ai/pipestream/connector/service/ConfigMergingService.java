package ai.pipestream.connector.service;

import ai.pipestream.connector.entity.Connector;
import ai.pipestream.connector.entity.DataSource;
import ai.pipestream.connector.intake.v1.DataSourceConfig;
import ai.pipestream.data.v1.HydrationConfig;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

/**
 * Service for merging Tier 1 configuration from Connector defaults and DataSource overrides.
 * <p>
 * Implements the 2-tier configuration model by merging:
 * - Connector defaults (service-level defaults for all datasources using this connector)
 * - DataSource overrides (instance-level overrides for a specific datasource)
 * - System defaults (fallback values when neither connector nor datasource provides a value)
 * <p>
 * Override semantics: DataSource overrides Connector, which overrides System defaults.
 */
@ApplicationScoped
public class ConfigMergingService {

    private static final Logger LOG = Logger.getLogger(ConfigMergingService.class);
    
    // System defaults
    private static final boolean SYSTEM_DEFAULT_PERSIST_PIPEDOC = true;
    private static final int SYSTEM_DEFAULT_MAX_INLINE_SIZE_BYTES = 1048576; // 1MB

    /**
     * Merges Tier 1 configuration from Connector defaults and DataSource overrides.
     * <p>
     * Merge order:
     * 1. Start with system defaults
     * 2. Apply connector defaults (override system defaults)
     * 3. Apply datasource overrides (override connector defaults)
     * 4. Deserialize datasource.global_config_proto if present (final override)
     *
     * @param connector Connector entity with default configuration
     * @param datasource DataSource entity with override configuration
     * @return Merged ConnectorGlobalConfig proto message
     */
    public DataSourceConfig.ConnectorGlobalConfig mergeTier1Config(Connector connector, DataSource datasource) {
        DataSourceConfig.ConnectorGlobalConfig.Builder merged = DataSourceConfig.ConnectorGlobalConfig.newBuilder();

        // Step 1: Start with system defaults
        applySystemDefaults(merged);

        // Step 2: Apply connector defaults if connector exists
        if (connector != null) {
            applyConnectorDefaults(merged, connector);
        }

        // Step 3: Apply datasource overrides from individual columns
        if (datasource != null) {
            applyDataSourceColumnOverrides(merged, datasource);
        }

        // Step 4: Deserialize and merge datasource.global_config_proto if present (highest priority)
        if (datasource != null && datasource.globalConfigProto != null && datasource.globalConfigProto.length > 0) {
            applyDataSourceProtoOverrides(merged, datasource);
        }

        return merged.build();
    }

    /**
     * Apply system defaults to the builder.
     */
    private void applySystemDefaults(DataSourceConfig.ConnectorGlobalConfig.Builder builder) {
        // System default for PersistenceConfig
        DataSourceConfig.PersistenceConfig persistenceConfig = DataSourceConfig.PersistenceConfig.newBuilder()
            .setPersistPipedoc(SYSTEM_DEFAULT_PERSIST_PIPEDOC)
            .setMaxInlineSizeBytes(SYSTEM_DEFAULT_MAX_INLINE_SIZE_BYTES)
            .build();
        builder.setPersistenceConfig(persistenceConfig);

        // HydrationConfig defaults to AUTO policy
        HydrationConfig hydrationConfig = HydrationConfig.newBuilder()
            .setDefaultHydrationPolicy(HydrationConfig.HydrationPolicy.HYDRATION_POLICY_AUTO)
            .build();
        builder.setHydrationConfig(hydrationConfig);
    }

    /**
     * Apply connector defaults to the builder.
     */
    private void applyConnectorDefaults(DataSourceConfig.ConnectorGlobalConfig.Builder builder, Connector connector) {
        // Build PersistenceConfig from connector defaults
        DataSourceConfig.PersistenceConfig.Builder persistenceBuilder = builder.getPersistenceConfig().toBuilder();
        
        if (connector.defaultPersistPipedoc != null) {
            persistenceBuilder.setPersistPipedoc(connector.defaultPersistPipedoc);
        }
        if (connector.defaultMaxInlineSizeBytes != null) {
            persistenceBuilder.setMaxInlineSizeBytes(connector.defaultMaxInlineSizeBytes);
        }
        builder.setPersistenceConfig(persistenceBuilder.build());

        // Apply custom_config defaults from connector
        if (connector.defaultCustomConfig != null && !connector.defaultCustomConfig.isEmpty() && !connector.defaultCustomConfig.equals("{}")) {
            try {
                Struct connectorCustomConfig = jsonStringToStruct(connector.defaultCustomConfig);
                // Set connector custom_config (will be merged with datasource column later)
                builder.setCustomConfig(connectorCustomConfig);
            } catch (Exception e) {
                LOG.warnf(e, "Failed to parse connector default custom config for connector %s: %s", 
                    connector.connectorId, connector.defaultCustomConfig);
            }
        }
    }

    /**
     * Apply datasource overrides from individual columns (before proto deserialization).
     */
    private void applyDataSourceColumnOverrides(DataSourceConfig.ConnectorGlobalConfig.Builder builder, DataSource datasource) {
        // Note: Currently, datasource entity doesn't have individual override columns for strongly-typed fields
        // All overrides come from global_config_proto. This method is a placeholder for future column-based overrides.

        // Apply custom_config override from datasource column (if present and not in proto)
        // This is applied before proto merge, so proto will override if present
        if (datasource.customConfig != null && !datasource.customConfig.isEmpty() && !datasource.customConfig.equals("{}")) {
            try {
                Struct datasourceCustomConfig = jsonStringToStruct(datasource.customConfig);
                // Merge with existing custom_config (connector defaults)
                if (builder.hasCustomConfig()) {
                    Struct merged = mergeStructs(builder.getCustomConfig(), datasourceCustomConfig);
                    builder.setCustomConfig(merged);
                } else {
                    builder.setCustomConfig(datasourceCustomConfig);
                }
            } catch (Exception e) {
                LOG.warnf(e, "Failed to parse datasource custom config for datasource %s: %s", 
                    datasource.datasourceId, datasource.customConfig);
            }
        }
    }

    /**
     * Deserialize and merge datasource.global_config_proto (highest priority override).
     * <p>
     * mergeFrom handles all strongly-typed fields recursively (PersistenceConfig, RetentionConfig, etc.).
     * For custom_config (Struct): we want replacement semantics, not merge semantics.
     */
    private void applyDataSourceProtoOverrides(DataSourceConfig.ConnectorGlobalConfig.Builder builder, DataSource datasource) {
        try {
            DataSourceConfig.ConnectorGlobalConfig protoConfig = 
                DataSourceConfig.ConnectorGlobalConfig.parseFrom(datasource.globalConfigProto);

            // Save proto's nested messages BEFORE mergeFrom
            // We'll use mergeFrom for the merge, then explicitly replace these if proto has them
            Struct protoCustomConfig = protoConfig.hasCustomConfig() ? protoConfig.getCustomConfig() : null;
            DataSourceConfig.PersistenceConfig protoPersistenceConfig = protoConfig.hasPersistenceConfig() ? protoConfig.getPersistenceConfig() : null;
            DataSourceConfig.RetentionConfig protoRetentionConfig = protoConfig.hasRetentionConfig() ? protoConfig.getRetentionConfig() : null;
            DataSourceConfig.EncryptionConfig protoEncryptionConfig = protoConfig.hasEncryptionConfig() ? protoConfig.getEncryptionConfig() : null;
            HydrationConfig protoHydrationConfig = protoConfig.hasHydrationConfig() ? protoConfig.getHydrationConfig() : null;

            // mergeFrom handles all strongly-typed fields (recursive merge on nested messages)
            builder.mergeFrom(protoConfig);

            // Explicitly replace nested messages if proto has them (ensures proto override takes precedence)
            // This is necessary because mergeFrom behavior may not always override as expected
            if (protoPersistenceConfig != null) {
                builder.setPersistenceConfig(protoPersistenceConfig);
            }
            if (protoRetentionConfig != null) {
                builder.setRetentionConfig(protoRetentionConfig);
            }
            if (protoEncryptionConfig != null) {
                builder.setEncryptionConfig(protoEncryptionConfig);
            }
            if (protoHydrationConfig != null) {
                builder.setHydrationConfig(protoHydrationConfig);
            }
            if (protoCustomConfig != null) {
                builder.setCustomConfig(protoCustomConfig);
            }

            LOG.debugf("Successfully deserialized and merged global_config_proto for datasource %s", datasource.datasourceId);
        } catch (InvalidProtocolBufferException e) {
            // Invalid bytes in global_config_proto is a data bug: something wrote non-ConnectorGlobalConfig (or corrupt) data.
            // Fail the request so callers see the error and the writer can be fixed; do not silently fall back.
            LOG.errorf(e, "Invalid or corrupt global_config_proto for datasource %s", datasource.datasourceId);
            throw new IllegalStateException(
                "Invalid global_config_proto for datasource " + datasource.datasourceId + ": " + e.getMessage(), e);
        }
    }

    /**
     * Convert JSON string to protobuf Struct.
     */
    private Struct jsonStringToStruct(String json) throws Exception {
        Struct.Builder structBuilder = Struct.newBuilder();
        JsonFormat.parser().merge(json, structBuilder);
        return structBuilder.build();
    }

    /**
     * Merge two Structs, with overrideStruct taking precedence (its fields override baseStruct).
     * Uses protobuf's built-in mergeFrom for deep, recursive merging of nested structures.
     */
    private Struct mergeStructs(Struct baseStruct, Struct overrideStruct) {
        // Use mergeFrom for deep merge (preserves nested fields, only overrides specified keys)
        return baseStruct.toBuilder()
                .mergeFrom(overrideStruct) // Deep, recursive merge
                .build();
    }
}

