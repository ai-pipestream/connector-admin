# S3 Dual-Bucket Design: Intake Staging vs Pipeline Processing

## Status: DRAFT / Design Exploration
## Date: 2026-04-07

---

## Problem Statement

Today, all document storage for an account flows through a single logical Drive,
which maps to one S3 bucket + prefix. Intake documents (raw connector uploads)
and pipeline-processed documents (parser output, semantic chunks, embeddings)
share the same bucket and namespace.

This creates several problems:

1. **Lifecycle coupling**: Deleting a pipeline graph's processed documents
   requires filtering within a shared namespace. No clean "drop everything
   from graph X" operation.

2. **Cost attribution**: Cannot separate storage costs for intake (source of
   truth) vs pipeline (derived, disposable copies).

3. **Retention mismatch**: Intake documents may need indefinite retention
   (legal, compliance) while pipeline copies are ephemeral and should be
   garbage-collected when a graph is deleted or re-processed.

4. **Customer-supplied buckets**: A customer may want intake to land in their
   own S3 bucket (BYOB) while pipeline processing uses platform-managed storage.

---

## Current Architecture

### Drive Entity (repository-service)

```
Drive {
  driveId: "{accountId}:default"
  s3Bucket: "pipestream"
  s3Prefix: "{accountId}"
  accountId: "..."
  credentialsRef: null  // uses platform credentials
}
```

### S3 Key Structure

```
{keyPrefix}/{driveName}/{accountId}/{connectorId}/{datasourceId}/{docId}/{nodeType}/{uuid}.pb

Examples:
  uploads/acct-123/acct-123/s3-conn/ds-hash/doc-001/intake/a1b2...pb
  uploads/acct-123/acct-123/s3-conn/ds-hash/doc-001/cluster-prod/b2c3...pb
```

### DataSource Entity (connector-admin)

```
DataSource {
  datasourceId: deterministic(accountId + connectorId)
  accountId: "..."
  driveName: "..."     // single drive reference
}
```

---

## Proposed Design: Two Drives Per Account

Each account gets two logical Drives:

| Drive | Purpose | Lifecycle | Owner |
|-------|---------|-----------|-------|
| **Intake Drive** | Source-of-truth documents from connectors | Retained until explicit delete | Account owner |
| **Pipeline Drive** | Derived documents from graph processing | Disposable; tied to graph lifecycle | Platform |

### Drive Naming Convention

```
Intake:   {accountId}:intake
Pipeline: {accountId}:pipeline
```

### S3 Path Hierarchy

```
Intake Drive:
  {bucket}/{accountId}/intake/{connectorId}/{datasourceId}/{docId}/{version}.pb

Pipeline Drive:
  {bucket}/{accountId}/pipeline/{clusterId}/{graphId}/{nodeId}/{docId}/{uuid}.pb
```

The intake path is organized by connector/datasource because that's the
ingestion dimension. The pipeline path is organized by cluster/graph/node
because that's the processing dimension.

### Bucket Options (Per Account)

```
Option A: Single bucket, two prefixes (default)
  s3://pipestream/acct-123/intake/...
  s3://pipestream/acct-123/pipeline/...

Option B: Two separate buckets (BYOB for intake)
  s3://customer-bucket/intake/...        // customer-owned, customer-KMS
  s3://pipestream-pipeline/acct-123/...  // platform-owned

Option C: Two separate buckets, both platform-managed
  s3://pipestream-intake/acct-123/...
  s3://pipestream-pipeline/acct-123/...
```

---

## Schema Changes

### connector-admin: DataSource Table

Replace single `drive_name` with two drive references:

```sql
ALTER TABLE datasources
  ADD COLUMN intake_drive_name VARCHAR(255),
  ADD COLUMN pipeline_drive_name VARCHAR(255);

-- Migrate existing data
UPDATE datasources
  SET intake_drive_name = drive_name,
      pipeline_drive_name = drive_name;

-- Eventually drop old column after migration
-- ALTER TABLE datasources DROP COLUMN drive_name;
```

### connector-admin: New Drives Table (Optional)

If connector-admin should own drive definitions (rather than delegating
to repository-service), add a local drives table:

```sql
CREATE TABLE account_drives (
  id             BIGSERIAL PRIMARY KEY,
  account_id     VARCHAR(255) NOT NULL,
  drive_type     VARCHAR(20)  NOT NULL,  -- 'INTAKE' or 'PIPELINE'
  drive_name     VARCHAR(255) NOT NULL,  -- logical name resolved by repo-service
  s3_bucket      VARCHAR(255),           -- NULL = use platform default
  s3_prefix      VARCHAR(255),           -- NULL = derive from account_id
  credentials_ref VARCHAR(255),          -- NULL = use platform credentials
  kms_key_ref    VARCHAR(255),           -- NULL = use platform KMS
  created_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
  updated_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),

  UNIQUE (account_id, drive_type)
);
```

**Design decision**: Should connector-admin own the drive definitions, or
should it only store drive_name references and let repository-service own
the Drive entity?

**Recommendation**: Connector-admin stores the *configuration intent*
(which buckets, which KMS keys, BYOB or platform). Repository-service
stores the *resolved Drive* entity with validated credentials. This keeps
credential management in one place (repo-service + Infisical) while letting
connector-admin express the business rules.

### repository-service: Drive Entity

The existing Drive entity already supports this. Just needs two drives
created per account instead of one:

```java
// DriveService.getOrCreateAccountDrives(accountId)
Drive intakeDrive = Drive.builder()
    .driveId(accountId + ":intake")
    .s3Bucket(resolvedIntakeBucket)
    .s3Prefix(accountId + "/intake")
    .accountId(accountId)
    .build();

Drive pipelineDrive = Drive.builder()
    .driveId(accountId + ":pipeline")
    .s3Bucket(resolvedPipelineBucket)
    .s3Prefix(accountId + "/pipeline")
    .accountId(accountId)
    .build();
```

---

## Proto Changes

### DataSourceConfig (connector_intake_service.proto)

```protobuf
message DataSourceConfig {
  // ... existing fields ...

  // NEW: separate drive references
  string intake_drive_name = 20;
  string pipeline_drive_name = 21;

  // DEPRECATED: single drive (backward compat)
  // string drive_name = 5;  // keep for migration period
}
```

### ConnectorGlobalConfig (connector_types.proto)

```protobuf
message PersistenceConfig {
  // ... existing fields ...

  // NEW: per-stage persistence policy
  bool persist_intake = 10;      // always true for staged connectors
  bool persist_pipeline = 11;    // controls whether pipeline copies are durable
}
```

---

## Lifecycle Rules

### Intake Drive Lifecycle

| Event | Action |
|-------|--------|
| Connector uploads document | Write to intake drive |
| Document re-uploaded (same doc_id) | Overwrite or version in intake drive |
| Connector deleted | Retain intake docs (configurable retention policy) |
| Account deleted | Cascade delete all intake docs (hard delete after grace period) |
| Retention policy expires | Background job purges expired docs |

### Pipeline Drive Lifecycle

| Event | Action |
|-------|--------|
| Engine saves processed doc | Write to pipeline drive |
| Graph re-processed | Old pipeline copies replaced |
| Graph deleted | **All pipeline docs for that graph are deleted** |
| Account deleted | Cascade delete all pipeline docs |
| Node removed from graph | Orphaned pipeline docs eligible for GC |

### Cascade Rule: Intake Deletion Propagates

Deleting an intake document MUST cascade to all derived pipeline copies.
The pipeline copies are worthless without their source.

```
DELETE intake doc-001
  -> Find all pipeline copies where source_doc_id = doc-001
  -> DELETE each pipeline copy
  -> DELETE any OpenSearch indexed copies
```

This cascade requires a reverse index from source_doc_id to pipeline copies,
which exists today in the `pipedocs` table via `(doc_id, account_id)` queries.

### Pipeline Deletion Does NOT Cascade Upward

Deleting pipeline copies (e.g., graph deletion) does NOT affect intake:

```
DELETE graph-42 pipeline copies
  -> Only deletes from pipeline drive
  -> Intake documents remain untouched
  -> Re-processing the graph would re-derive from intake
```

---

## Drive Resolution Flow

### At Intake Time

```
Connector uploads document
  -> Intake service resolves Tier 1 config
  -> Config has intake_drive_name (or falls back to drive_name)
  -> Passes drive_name to repository-service
  -> Repository resolves Drive entity
  -> Writes to intake bucket/prefix
```

### At Pipeline Processing Time

```
Engine processes document at node N
  -> Engine calls repo-service savePipeDoc()
  -> Passes pipeline_drive_name from stream metadata
     (or derives from graph's cluster_id)
  -> Repository resolves Drive entity
  -> Writes to pipeline bucket/prefix
```

### How pipeline_drive_name Gets to the Engine

Options:

**Option A**: Engine reads from DataSourceConfig (already fetched during intake)
- Pro: No new wire protocol
- Con: Engine must cache datasource config

**Option B**: Pipeline drive name flows through PipeStream metadata
- Pro: Self-contained, no external lookups
- Con: Wire overhead

**Option C**: Pipeline drive is always `{accountId}:pipeline` by convention
- Pro: Zero config, zero wire overhead
- Con: Less flexible for BYOB pipeline buckets

**Recommendation**: Option C for now. Convention-based. The pipeline drive
is always platform-managed. If BYOB pipeline buckets are needed later,
switch to Option B.

---

## BYOB (Bring Your Own Bucket) Support

For customers who want their intake documents in their own S3 bucket:

### Configuration in connector-admin

```json
{
  "intake_drive": {
    "bucket": "customer-legal-docs",
    "region": "eu-west-1",
    "credentials_ref": "infisical://customer-123/s3-intake",
    "kms_key_ref": "arn:aws:kms:eu-west-1:111:key/abc-123"
  },
  "pipeline_drive": null  // uses platform default
}
```

### Credential Storage

- Customer S3 credentials stored in Infisical (secret manager)
- `credentials_ref` is a Infisical path, not raw credentials
- Repository-service resolves credentials at runtime
- Rotation handled via Infisical policies

### Trust Boundary

```
Customer Bucket (intake):
  - Customer owns encryption keys
  - Platform has write-only access (or customer grants cross-account role)
  - Customer can audit access via CloudTrail
  - Customer can add their own lifecycle/retention policies

Platform Bucket (pipeline):
  - Platform owns everything
  - Customer has no direct access
  - Platform manages lifecycle via graph deletion
```

---

## Migration Path

### Phase 1: Dual Drive Creation (Non-Breaking)

1. Add `intake_drive_name` and `pipeline_drive_name` columns to datasources
2. Default both to current `drive_name` value
3. Repository-service creates `{accountId}:intake` and `{accountId}:pipeline`
   drives, both pointing to same bucket with different prefixes
4. No behavior change yet — all writes go through existing path

### Phase 2: Intake Path Separation

1. Connector-intake-service uses `intake_drive_name` for uploads
2. New intake documents land in `/intake/` prefix
3. Existing documents remain in old path (backward compat)
4. Reads check both paths until migration complete

### Phase 3: Pipeline Path Separation

1. Engine uses `pipeline_drive_name` for savePipeDoc
2. New pipeline copies land in `/pipeline/{clusterId}/{graphId}/` prefix
3. Graph deletion can now prefix-delete entire graph subtrees

### Phase 4: Cleanup

1. Background migration job moves old-path docs to new-path
2. Remove `drive_name` column from datasources
3. Remove backward-compat path resolution

---

## Open Questions

1. **Who creates the drives?** When an account is created, should
   connector-admin emit an event that repository-service consumes to
   create the two drives? Or should repository-service lazily create
   them on first use (current pattern)?

2. **Per-connector drives or per-account drives?** The framing says
   "a drive concept per connector (not just per account)." Should each
   connector/datasource get its own drive, or is account-level sufficient?
   Per-connector drives enable different S3 buckets per connector within
   one account but add complexity.

3. **Graph deletion cascade scope?** When a graph is deleted, should we
   delete all pipeline docs for that graph across all accounts, or is
   graph deletion always account-scoped?

4. **S3 Lifecycle Policies vs Application-Level GC?** Should pipeline
   cleanup use S3 lifecycle rules (TTL on prefix) or application-level
   garbage collection (query + delete)?

5. **Versioning?** Should intake documents be versioned in S3 (enabling
   rollback to previous crawl state) or is overwrite semantics sufficient?

---

## Impact Analysis

| Service | Changes Required | Scope |
|---------|-----------------|-------|
| connector-admin | Schema migration, dual drive fields in DataSource, proto update | Medium |
| connector-intake-service | Use `intake_drive_name` from config | Small |
| repository-service | Create two drives per account, path structure update | Medium |
| pipestream-engine | Use `pipeline_drive_name` for savePipeDoc | Small |
| pipestream-protos | New fields in DataSourceConfig, PersistenceConfig | Small |
| opensearch-sink | No change (reads from pipeline path via repo-service) | None |
| Frontend (Vue) | Drive management UI in account settings | Medium |

---

## Review Notes (Round 1)

### Accepted Critiques

**1. Drop the `account_drives` table from connector-admin (Phase 1).**
The table is premature. Convention-based drives (`{accountId}:intake`,
`{accountId}:pipeline`) created lazily by repository-service is sufficient.
Connector-admin should NOT own drive definitions in Phase 1. The
`account_drives` SQL in the Schema Changes section above is moved to
"Future: BYOB Phase" — only needed when customers bring their own buckets.

**2. Account-level drives, not per-connector.**
Per-connector drives are YAGNI. One intake drive + one pipeline drive per
account is the right granularity. Per-connector isolation is a BYOB concern
and belongs in a later phase. Open Question #2 is resolved: account-level.

**3. Graph deletion requires graph ID in the S3 key path.**
This is the hardest part. Today's S3 key structure is:
```
uploads/{driveName}/{accountId}/{connectorId}/{datasourceId}/{docId}/{nodeType}/{uuid}.pb
```
There is no `graphId` in the path. The `nodeType` is either "intake" or a
cluster ID — not a graph ID. For prefix-delete on graph deletion to work,
the pipeline path MUST include `{graphId}`:
```
{accountId}/pipeline/{clusterId}/{graphId}/{docId}/{nodeId}/{uuid}.pb
```
This means `DocumentStorageService.buildObjectKeyBase()` in repository-service
must be updated to include graph ID when writing pipeline copies. The graph ID
is available in `PipeStream.metadata.graphId` which flows through the engine.
This is a **prerequisite** for Phase 3 and should be designed carefully.

**4. Cascade reverse index is fragile — use S3 prefix-delete instead.**
The doc claimed `pipedocs` table has a reverse index from source_doc_id to
pipeline copies. It doesn't — the deterministic UUID is
`(docId + graphAddressId + accountId)` with no explicit FK back to intake.
Querying by `doc_id` across all graph locations works but is expensive.

Better approach: For graph deletion, use S3 prefix-delete on
`{accountId}/pipeline/{clusterId}/{graphId}/` (cheap, O(1) per graph).
For individual intake doc deletion cascade, query `pipedocs` by
`(doc_id, account_id)` where `cluster_id IS NOT NULL` — this is indexed
and fast for single-doc lookups.

**5. S3 Lifecycle + Application GC = use both.**
S3 lifecycle rules as a safety net (TTL on pipeline prefix, e.g., 90 days).
Application-level GC for immediate cleanup on graph deletion. They're
complementary. Open Question #4 is resolved: both.

**6. Proto impact is understated.**
Adding `intake_drive_name` / `pipeline_drive_name` to DataSourceConfig
touches every service that reads datasource config. But per the revised
Phase 1 (convention-only), **no proto changes are needed in Phase 1**.
Proto changes move to Phase 2 (BYOB support). Impact table updated.

### Revised Phase 1 (Minimal)

Phase 1 requires **zero schema changes and zero proto changes**:

1. Repository-service: `DriveService.getOrCreateDefaultDrive(accountId)`
   becomes `getOrCreateDrives(accountId)` — creates both
   `{accountId}:intake` and `{accountId}:pipeline` lazily, same bucket,
   different prefixes.
2. Connector-intake-service: passes `{accountId}:intake` as drive name
   (convention, no config lookup needed).
3. Engine: passes `{accountId}:pipeline` as drive name in `savePipeDoc()`
   (convention, no config lookup needed).
4. No new tables, no new proto fields, no migrations.

This gets separation of intake vs pipeline storage with zero cross-service
coordination. BYOB, custom buckets, and per-connector drives come later
when there's a real customer requirement.

### Revised Impact Analysis (Phase 1 Only)

| Service | Changes Required | Scope |
|---------|-----------------|-------|
| connector-admin | None | None |
| connector-intake-service | Use `{accountId}:intake` convention for drive name | Trivial |
| repository-service | Lazy-create two drives per account instead of one | Small |
| pipestream-engine | Use `{accountId}:pipeline` convention for drive name | Trivial |
| pipestream-protos | None | None |
| opensearch-sink | None | None |
| Frontend (Vue) | None | None |

### Remaining Open Questions

1. ~~Who creates the drives?~~ **Resolved**: Repo-service, lazily.
2. ~~Per-connector or per-account?~~ **Resolved**: Per-account.
3. **Graph deletion cascade scope?** Still open. Likely account-scoped
   (a graph belongs to an account).
4. ~~S3 Lifecycle vs App GC?~~ **Resolved**: Both.
5. **Versioning?** Still open. Overwrite is fine for now; S3 versioning
   adds cost and complexity for a feature nobody has asked for yet.
6. **NEW: S3 key path retrofit.** How and when do we add `graphId` to
   the pipeline S3 key? This blocks prefix-delete for graph cleanup.
   Options: (a) new path for new docs only, old path grandfathered;
   (b) background migration job rewrites all existing keys. Option (a)
   is simpler but means two path formats coexist until old docs expire.
