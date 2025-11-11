package ai.pipestream.connector.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

/**
 * Connector metadata structure representing flexible configuration stored as JSON in the database.
 * <p>
 * This class encapsulates connector-specific configuration including document storage paths,
 * operational limits, and default metadata that gets applied to all ingested documents.
 * The metadata is serialized to JSON and stored in the {@link ai.pipestream.connector.entity.Connector#metadata}
 * field, allowing for schema flexibility without database migrations.
 *
 * <h2>Configuration Categories</h2>
 * <ul>
 *   <li><b>Storage Configuration</b>: S3 bucket and path for document storage</li>
 *   <li><b>Operational Limits</b>: File size and rate limiting constraints</li>
 *   <li><b>Document Defaults</b>: Key-value metadata applied to all documents</li>
 * </ul>
 *
 * <h2>JSON Schema Example</h2>
 * <pre>
 * {
 *   "s3Bucket": "pipestream-docs",
 *   "s3BasePath": "connectors/confluence/acme-corp",
 *   "maxFileSize": 52428800,
 *   "rateLimitPerMinute": 60,
 *   "defaultMetadata": {
 *     "source": "confluence",
 *     "organization": "acme-corp",
 *     "tier": "enterprise"
 *   }
 * }
 * </pre>
 *
 * <h2>Usage Patterns</h2>
 * <pre>
 * // Creating metadata
 * ConnectorMetadata metadata = new ConnectorMetadata();
 * metadata.setS3Bucket("company-docs");
 * metadata.setS3BasePath("connectors/sharepoint");
 * metadata.setMaxFileSize(100 * 1024 * 1024); // 100MB
 * metadata.setRateLimitPerMinute(120);
 * metadata.setDefaultMetadata(Map.of("source", "sharepoint"));
 * String json = metadata.toJson();
 *
 * // Parsing existing metadata
 * ConnectorMetadata existing = ConnectorMetadata.fromJson(connector.metadata);
 * existing.setMaxFileSize(200 * 1024 * 1024); // Update limit
 * connector.metadata = existing.toJson();
 *
 * // Merging metadata (in service layer)
 * ConnectorMetadata current = ConnectorMetadata.fromJson(connector.metadata);
 * current.getDefaultMetadata().putAll(newMetadata);
 * connector.metadata = current.toJson();
 * </pre>
 *
 * <h2>Field Descriptions</h2>
 * <ul>
 *   <li><b>s3Bucket</b>: Target S3 bucket name for document storage</li>
 *   <li><b>s3BasePath</b>: Base path/prefix within the bucket (e.g., "connectors/type/name")</li>
 *   <li><b>maxFileSize</b>: Maximum allowed file size in bytes (0 = no limit)</li>
 *   <li><b>rateLimitPerMinute</b>: Maximum API requests per minute (0 = no limit)</li>
 *   <li><b>defaultMetadata</b>: Key-value pairs applied to all ingested documents for tagging/filtering</li>
 * </ul>
 *
 * <h2>Immutability and Thread Safety</h2>
 * This class is mutable and not thread-safe. Instances should not be shared across threads
 * without external synchronization. Database updates are protected by JPA transaction boundaries.
 *
 * @see ai.pipestream.connector.entity.Connector#metadata
 * @see ai.pipestream.connector.service.ConnectorAdminServiceImpl#updateConnector
 * @since 1.0.0
 */
public class ConnectorMetadata {

    private String s3Bucket = "";
    private String s3BasePath = "";
    private long maxFileSize = 0;
    private long rateLimitPerMinute = 0;
    private Map<String, String> defaultMetadata = new HashMap<>();

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public ConnectorMetadata() {}

    // Getters and setters
    public String getS3Bucket() { return s3Bucket; }
    public void setS3Bucket(String s3Bucket) { this.s3Bucket = s3Bucket; }

    public String getS3BasePath() { return s3BasePath; }
    public void setS3BasePath(String s3BasePath) { this.s3BasePath = s3BasePath; }

    public long getMaxFileSize() { return maxFileSize; }
    public void setMaxFileSize(long maxFileSize) { this.maxFileSize = maxFileSize; }

    public long getRateLimitPerMinute() { return rateLimitPerMinute; }
    public void setRateLimitPerMinute(long rateLimitPerMinute) { this.rateLimitPerMinute = rateLimitPerMinute; }

    public Map<String, String> getDefaultMetadata() { return defaultMetadata; }
    public void setDefaultMetadata(Map<String, String> defaultMetadata) { this.defaultMetadata = defaultMetadata; }

    /**
     * Serializes this metadata instance to a JSON string.
     *
     * Side effects:
     * - None.
     *
     * @return JSON representation suitable for database storage.
     * @throws RuntimeException if serialization fails; wraps {@code JsonProcessingException}.
     */
    public String toJson() {
        try {
            return MAPPER.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize connector metadata", e);
        }
    }

    /**
     * Deserializes a JSON string into a `ConnectorMetadata` instance.
     *
     * Reactive semantics:
     * - Not reactive; simple in-memory conversion using Jackson.
     *
     * Side effects:
     * - None.
     *
     * @param json JSON representation as stored in the database; accepts null, empty, or "{}" to mean default metadata.
     * @return Parsed `ConnectorMetadata` instance; returns a new default instance when input is null/empty/"{}".
     * @throws RuntimeException if deserialization fails; wraps {@code JsonProcessingException}.
     */
    public static ConnectorMetadata fromJson(String json) {
        if (json == null || json.isEmpty() || json.equals("{}")) {
            return new ConnectorMetadata();
        }
        try {
            return MAPPER.readValue(json, ConnectorMetadata.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to deserialize connector metadata", e);
        }
    }
}
