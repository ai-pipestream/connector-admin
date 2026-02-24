package ai.pipestream.connector.service;

import ai.pipestream.connector.entity.Connector;
import ai.pipestream.connector.entity.DataSource;
import ai.pipestream.connector.intake.v1.DataSourceConfig;
import ai.pipestream.data.v1.HydrationConfig;
import com.google.protobuf.Struct;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for ConfigMergingService.
 */
@QuarkusTest
public class ConfigMergingServiceTest {

    @Inject
    ConfigMergingService configMergingService;

    @Test
    void testMergeTier1Config_SystemDefaultsOnly() {
        // No connector, no datasource - should get system defaults
        DataSourceConfig.ConnectorGlobalConfig merged = 
            configMergingService.mergeTier1Config(null, null);

        assertNotNull(merged);
        assertTrue(merged.hasPersistenceConfig());
        assertEquals(true, merged.getPersistenceConfig().getPersistPipedoc()); // System default
        assertEquals(1048576, merged.getPersistenceConfig().getMaxInlineSizeBytes()); // 1MB system default
        assertTrue(merged.hasHydrationConfig());
        assertEquals(HydrationConfig.HydrationPolicy.HYDRATION_POLICY_AUTO, 
            merged.getHydrationConfig().getDefaultHydrationPolicy());
    }

    @Test
    void testMergeTier1Config_ConnectorDefaultsOnly() {
        Connector connector = new Connector();
        connector.connectorId = "test-connector";
        connector.defaultPersistPipedoc = false;
        connector.defaultMaxInlineSizeBytes = 2097152; // 2MB
        connector.defaultCustomConfig = "{\"parse_images\":true}";

        DataSource datasource = new DataSource();
        datasource.datasourceId = "test-datasource";
        datasource.connectorId = connector.connectorId;

        DataSourceConfig.ConnectorGlobalConfig merged = 
            configMergingService.mergeTier1Config(connector, datasource);

        assertNotNull(merged);
        assertTrue(merged.hasPersistenceConfig());
        assertEquals(false, merged.getPersistenceConfig().getPersistPipedoc()); // From connector
        assertEquals(2097152, merged.getPersistenceConfig().getMaxInlineSizeBytes()); // From connector
        assertTrue(merged.hasCustomConfig());
        assertTrue(merged.getCustomConfig().getFieldsMap().containsKey("parse_images"));
        assertEquals(true, merged.getCustomConfig().getFieldsMap().get("parse_images").getBoolValue());
    }

    @Test
    void testMergeTier1Config_DataSourceOverrides() throws Exception {
        Connector connector = new Connector();
        connector.connectorId = "test-connector";
        connector.defaultPersistPipedoc = true;
        connector.defaultMaxInlineSizeBytes = 1048576; // 1MB
        connector.defaultCustomConfig = "{\"parse_images\":true,\"include_history\":false}";

        DataSource datasource = new DataSource();
        datasource.datasourceId = "test-datasource";
        datasource.connectorId = connector.connectorId;
        datasource.customConfig = "{\"parse_images\":false}"; // Override parse_images

        // Create proto override for datasource
        DataSourceConfig.ConnectorGlobalConfig datasourceOverride = 
            DataSourceConfig.ConnectorGlobalConfig.newBuilder()
                .setPersistenceConfig(
                    DataSourceConfig.PersistenceConfig.newBuilder()
                        .setPersistPipedoc(false) // Override connector default
                        .setMaxInlineSizeBytes(3145728) // 3MB override
                        .build()
                )
                .build();
        datasource.globalConfigProto = datasourceOverride.toByteArray();

        DataSourceConfig.ConnectorGlobalConfig merged = 
            configMergingService.mergeTier1Config(connector, datasource);

        assertNotNull(merged);
        // Proto override should win
        assertTrue(merged.hasPersistenceConfig());
        assertEquals(false, merged.getPersistenceConfig().getPersistPipedoc()); // From datasource proto override
        assertEquals(3145728, merged.getPersistenceConfig().getMaxInlineSizeBytes()); // From datasource proto override
        
        // Custom config should be merged from connector defaults + datasource column overrides
        // because the proto override in this test does not include custom_config.
        assertTrue(merged.hasCustomConfig());
        Struct customConfig = merged.getCustomConfig();
        assertEquals(false, customConfig.getFieldsMap().get("parse_images").getBoolValue()); // From datasource column
        assertEquals(false, customConfig.getFieldsMap().get("include_history").getBoolValue()); // From connector defaults
    }

    @Test
    void testMergeTier1Config_PartialOverrides() {
        Connector connector = new Connector();
        connector.connectorId = "test-connector";
        connector.defaultPersistPipedoc = true;
        connector.defaultMaxInlineSizeBytes = 1048576;

        DataSource datasource = new DataSource();
        datasource.datasourceId = "test-datasource";
        datasource.connectorId = connector.connectorId;
        // No overrides - should use connector defaults

        DataSourceConfig.ConnectorGlobalConfig merged = 
            configMergingService.mergeTier1Config(connector, datasource);

        assertNotNull(merged);
        assertTrue(merged.hasPersistenceConfig());
        assertEquals(true, merged.getPersistenceConfig().getPersistPipedoc()); // From connector
        assertEquals(1048576, merged.getPersistenceConfig().getMaxInlineSizeBytes()); // From connector
    }

    @Test
    void testMergeTier1Config_CustomConfigMerging() {
        Connector connector = new Connector();
        connector.connectorId = "test-connector";
        connector.defaultCustomConfig = "{\"parse_images\":true,\"include_history\":false}";

        DataSource datasource = new DataSource();
        datasource.datasourceId = "test-datasource";
        datasource.connectorId = connector.connectorId;
        datasource.customConfig = "{\"parse_images\":false,\"extract_urls\":true}"; // Override parse_images, add extract_urls

        DataSourceConfig.ConnectorGlobalConfig merged = 
            configMergingService.mergeTier1Config(connector, datasource);

        assertNotNull(merged);
        assertTrue(merged.hasCustomConfig());
        Struct customConfig = merged.getCustomConfig();
        
        // datasource.parse_images should override connector.parse_images
        assertTrue(customConfig.getFieldsMap().containsKey("parse_images"));
        assertEquals(false, customConfig.getFieldsMap().get("parse_images").getBoolValue());
        
        // connector.include_history should remain
        assertTrue(customConfig.getFieldsMap().containsKey("include_history"));
        assertEquals(false, customConfig.getFieldsMap().get("include_history").getBoolValue());
        
        // datasource.extract_urls should be present
        assertTrue(customConfig.getFieldsMap().containsKey("extract_urls"));
        assertEquals(true, customConfig.getFieldsMap().get("extract_urls").getBoolValue());
    }

    @Test
    void testMergeTier1Config_ProtoDeserialization() throws Exception {
        Connector connector = new Connector();
        connector.connectorId = "test-connector";
        connector.defaultPersistPipedoc = true;

        // Create proto with specific config
        DataSourceConfig.ConnectorGlobalConfig protoConfig = 
            DataSourceConfig.ConnectorGlobalConfig.newBuilder()
                .setPersistenceConfig(
                    DataSourceConfig.PersistenceConfig.newBuilder()
                        .setPersistPipedoc(false)
                        .setMaxInlineSizeBytes(5242880) // 5MB
                        .build()
                )
                .setHydrationConfig(
                    HydrationConfig.newBuilder()
                        .setDefaultHydrationPolicy(HydrationConfig.HydrationPolicy.HYDRATION_POLICY_ALWAYS_REF)
                        .build()
                )
                .build();

        DataSource datasource = new DataSource();
        datasource.datasourceId = "test-datasource";
        datasource.connectorId = connector.connectorId;
        datasource.globalConfigProto = protoConfig.toByteArray();

        DataSourceConfig.ConnectorGlobalConfig merged = 
            configMergingService.mergeTier1Config(connector, datasource);

        assertNotNull(merged);
        // Proto should override connector
        assertTrue(merged.hasPersistenceConfig());
        assertEquals(false, merged.getPersistenceConfig().getPersistPipedoc()); // From proto
        assertEquals(5242880, merged.getPersistenceConfig().getMaxInlineSizeBytes()); // From proto
        assertTrue(merged.hasHydrationConfig());
        assertEquals(HydrationConfig.HydrationPolicy.HYDRATION_POLICY_ALWAYS_REF, 
            merged.getHydrationConfig().getDefaultHydrationPolicy()); // From proto
    }

    @Test
    void testMergeTier1Config_NullHandling() {
        // Test with null connector
        DataSource datasource = new DataSource();
        datasource.datasourceId = "test-datasource";

        DataSourceConfig.ConnectorGlobalConfig merged = 
            configMergingService.mergeTier1Config(null, datasource);

        assertNotNull(merged);
        // Should have system defaults
        assertTrue(merged.hasPersistenceConfig());
        assertEquals(true, merged.getPersistenceConfig().getPersistPipedoc());
    }

    /**
     * When global_config_proto contains invalid/corrupt bytes, the service throws.
     * Invalid proto in the DB is a bug (wrong or corrupt data was written); we fail fast instead of silently falling back.
     */
    @Test
    void testMergeTier1Config_InvalidProtoThrows() {
        Connector connector = new Connector();
        connector.connectorId = "test-connector";
        connector.defaultPersistPipedoc = true;

        DataSource datasource = new DataSource();
        datasource.datasourceId = "test-datasource";
        datasource.connectorId = connector.connectorId;
        datasource.globalConfigProto = new byte[]{1, 2, 3}; // Invalid proto bytes

        IllegalStateException thrown = assertThrows(IllegalStateException.class, () ->
            configMergingService.mergeTier1Config(connector, datasource));

        assertTrue(thrown.getMessage().contains("test-datasource"));
        assertTrue(thrown.getCause() instanceof com.google.protobuf.InvalidProtocolBufferException);
    }
}

