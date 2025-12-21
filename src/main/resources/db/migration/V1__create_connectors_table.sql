-- Connector types (pre-seeded templates)
-- Connectors are reusable across accounts - each account creates a DataSource binding
CREATE TABLE IF NOT EXISTS connectors (
    connector_id VARCHAR(255) PRIMARY KEY,
    connector_type VARCHAR(100) NOT NULL UNIQUE,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    management_type VARCHAR(20) NOT NULL DEFAULT 'UNMANAGED',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for common queries
CREATE INDEX idx_connectors_type ON connectors(connector_type);
CREATE INDEX idx_connectors_management_type ON connectors(management_type);

-- Pre-seed connector types
-- connector_id = UUID.nameUUIDFromBytes(connector_type.getBytes(UTF_8))
INSERT INTO connectors (connector_id, connector_type, name, description, management_type)
VALUES
    ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 's3', 'S3 Bucket Crawler', 'Crawl documents from Amazon S3 buckets', 'UNMANAGED'),
    ('b1ffc0aa-0d1c-5f09-cc7e-7cc0ce491b22', 'file-crawler', 'File System Crawler', 'Crawl documents from local or network file systems', 'UNMANAGED')
ON CONFLICT (connector_id) DO NOTHING;
