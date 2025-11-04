-- Create connectors table
CREATE TABLE IF NOT EXISTS connectors (
    connector_id VARCHAR(255) PRIMARY KEY,
    connector_name VARCHAR(255) NOT NULL UNIQUE,
    connector_type VARCHAR(100) NOT NULL,
    description TEXT,
    api_key_hash VARCHAR(255) NOT NULL,
    active TINYINT(1) NOT NULL DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    last_rotated_at TIMESTAMP NULL,
    metadata JSON
);

-- Create indexes for common queries
CREATE INDEX idx_connectors_active ON connectors(active);
CREATE INDEX idx_connectors_name ON connectors(connector_name);
CREATE INDEX idx_connectors_type ON connectors(connector_type);
