-- Extend connectors table with configuration and registration fields
-- Supports 2-tier configuration model: Tier 1 (global/default) configuration

-- References connector_config_schemas for JSON Schema validation of custom config
ALTER TABLE connectors ADD COLUMN IF NOT EXISTS custom_config_schema_id VARCHAR(255);

-- Strongly typed default values for Tier 1 configuration
-- These are defaults that apply to all DataSources using this connector
-- Stored as individual columns for queryability (alternative to serialized protobuf)
ALTER TABLE connectors ADD COLUMN IF NOT EXISTS default_persist_pipedoc BOOLEAN DEFAULT TRUE;
ALTER TABLE connectors ADD COLUMN IF NOT EXISTS default_max_inline_size_bytes INTEGER DEFAULT 1048576;  -- 1MB default

-- Custom config defaults (JSON Schema-validated)
-- Default values for connector-specific custom configuration
ALTER TABLE connectors ADD COLUMN IF NOT EXISTS default_custom_config JSONB;

-- Display and metadata fields for UI/admin
ALTER TABLE connectors ADD COLUMN IF NOT EXISTS display_name VARCHAR(255);
ALTER TABLE connectors ADD COLUMN IF NOT EXISTS owner VARCHAR(255);
ALTER TABLE connectors ADD COLUMN IF NOT EXISTS documentation_url TEXT;
ALTER TABLE connectors ADD COLUMN IF NOT EXISTS tags TEXT[];

-- Create index for schema reference
CREATE INDEX IF NOT EXISTS idx_connectors_config_schema ON connectors(custom_config_schema_id);

-- Add foreign key constraint for schema reference (only if it doesn't exist)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'fk_connectors_config_schema'
    ) THEN
        ALTER TABLE connectors 
            ADD CONSTRAINT fk_connectors_config_schema 
            FOREIGN KEY (custom_config_schema_id) 
            REFERENCES connector_config_schemas(schema_id);
    END IF;
END $$;

