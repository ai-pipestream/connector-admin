-- DataSources (account + connector type binding)
-- Each account can have one DataSource per connector type
-- datasource_id = hash(account_id + connector_id)
CREATE TABLE IF NOT EXISTS datasources (
    datasource_id VARCHAR(255) PRIMARY KEY,
    account_id VARCHAR(255) NOT NULL,
    connector_id VARCHAR(255) NOT NULL,
    name VARCHAR(255) NOT NULL,
    api_key_hash VARCHAR(255) NOT NULL,
    drive_name VARCHAR(255) NOT NULL,
    metadata JSONB DEFAULT '{}',
    max_file_size BIGINT DEFAULT 0,
    rate_limit_per_minute BIGINT DEFAULT 0,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    status_reason VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    last_rotated_at TIMESTAMP WITH TIME ZONE,
    CONSTRAINT fk_datasource_connector FOREIGN KEY (connector_id) REFERENCES connectors(connector_id),
    CONSTRAINT uq_account_connector UNIQUE (account_id, connector_id)
);

-- Create indexes for common queries
CREATE INDEX idx_datasources_account ON datasources(account_id);
CREATE INDEX idx_datasources_connector ON datasources(connector_id);
CREATE INDEX idx_datasources_active ON datasources(active);
CREATE INDEX idx_datasources_drive ON datasources(drive_name);
