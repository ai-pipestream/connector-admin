-- Connector configuration schemas (JSON Schema for custom config)
-- Stores JSON Schema definitions for connector-specific custom configuration
-- Similar to module registration pattern - enables JSON Forms UI generation
CREATE TABLE IF NOT EXISTS connector_config_schemas (
    schema_id VARCHAR(255) PRIMARY KEY,
    connector_id VARCHAR(255) NOT NULL,
    schema_version VARCHAR(100) NOT NULL,
    
    -- JSON Schema ONLY for custom/connector-specific configuration
    -- Known fields (persistence, retention, encryption, hydration) are strongly typed in protobuf
    custom_config_schema JSONB NOT NULL,      -- Tier 1: Custom config JSON Schema
    node_custom_config_schema JSONB NOT NULL, -- Tier 2: Node custom config JSON Schema
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(255),
    
    -- Apicurio integration (like modules)
    apicurio_artifact_id VARCHAR(255),
    apicurio_global_id BIGINT,
    sync_status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    last_sync_attempt TIMESTAMP WITH TIME ZONE,
    sync_error VARCHAR(255),
    
    CONSTRAINT unique_connector_schema_version UNIQUE(connector_id, schema_version),
    CONSTRAINT fk_config_schema_connector FOREIGN KEY (connector_id) REFERENCES connectors(connector_id)
);

-- Create indexes for common queries
CREATE INDEX idx_connector_config_schemas_connector ON connector_config_schemas(connector_id);
CREATE INDEX idx_connector_config_schemas_sync_status ON connector_config_schemas(sync_status);

