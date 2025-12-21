-- Pre-seed connector types for testing
-- These UUIDs match the deterministic IDs used in V1__create_connectors_table.sql

INSERT INTO connectors (connector_id, connector_type, name, description, management_type, created_at)
VALUES
    ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 's3', 'S3 Connector', 'Amazon S3 and compatible object storage connector', 'UNMANAGED', CURRENT_TIMESTAMP),
    ('b1ffc0aa-0d1c-5f09-cc7e-7cc0ce491b22', 'file-crawler', 'File Crawler', 'Local filesystem crawler for development and testing', 'UNMANAGED', CURRENT_TIMESTAMP);
