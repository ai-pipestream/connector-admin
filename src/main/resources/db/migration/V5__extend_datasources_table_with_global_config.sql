-- Extend datasources table with Tier 1 (global) configuration overrides
-- DataSource-level overrides of Connector defaults
-- Tier 2 (per-node) configuration is stored in the Engine's graph configuration

-- Strongly typed configuration overrides (Tier 1)
-- Serialized ConnectorGlobalConfig protobuf message
-- Contains: PersistenceConfig, RetentionConfig, EncryptionConfig, HydrationConfig
ALTER TABLE datasources ADD COLUMN IF NOT EXISTS global_config_proto BYTEA;

-- Custom config overrides (JSON Schema-validated)
-- Instance-level custom configuration that overrides connector defaults
ALTER TABLE datasources ADD COLUMN IF NOT EXISTS custom_config JSONB;

-- Schema version used for validation
ALTER TABLE datasources ADD COLUMN IF NOT EXISTS custom_config_schema_id VARCHAR(255);

-- Create index for schema reference
CREATE INDEX IF NOT EXISTS idx_datasources_config_schema ON datasources(custom_config_schema_id);

-- Add foreign key constraint for schema reference (only if it doesn't exist)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'fk_datasource_config_schema'
    ) THEN
        ALTER TABLE datasources 
            ADD CONSTRAINT fk_datasource_config_schema 
            FOREIGN KEY (custom_config_schema_id) 
            REFERENCES connector_config_schemas(schema_id);
    END IF;
END $$;

