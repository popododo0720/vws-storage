# 0002: Bucket names use one global namespace, not an account-regional one

- Status: accepted
- Date: 2026-10-30
- Issue: #155 (follow-up of #154). Parity gap: #170

## Context

S3 `CreateBucket` can create a general purpose bucket in one of two namespaces,
selected by `x-amz-bucket-namespace`.

- `global` is the default. A name is unique across all AWS accounts and
  regions.
- `account-regional` is a subdivision of the global namespace that only one AWS
  account can create buckets in. A name must end with
  `-{12-digit-account-id}-{region}-an`, so the full name stays unique across
  AWS and only the prefix can repeat across accounts
  (AWS SDK `CreateBucket`, `BucketNamespace`, and the S3 User Guide page
  "Namespaces for general purpose buckets").

VWS #154 made bucket names one global namespace across projects. The engine
reads a bucket by `projects/{project}/buckets/{name}` and refuses a name that
another project holds, in any state, with `ALREADY_EXISTS`. The S3-compatible
backend does not call AWS general purpose S3. It maps each VWS bucket to
`vws-` plus a hash of the full VWS name in the configured RGW or MinIO store
(`internal/backend/s3/s3.go`, `bucketID`).

A VWS bucket name is a resource id. It starts with a lowercase letter, ends with
a lowercase letter or digit, and carries no account or region suffix. The
stored `Bucket.Region` comes from the request, but the bucket key does not
include the region.

## Options

1. **Implement account-regional now.** Add a namespace field to the bucket API,
   validate the account-regional suffix, put the region in the stored key, send
   `x-amz-bucket-namespace`, and migrate existing bucket rows.
2. **Keep the global namespace and record the difference.** Leave the API and
   the stored key unchanged. Add a name index so create does not scan every
   bucket row.

## Decision

Option 2. VWS keeps one global bucket namespace across projects. The
missing account-regional namespace is a documented parity gap, tracked by
#170.

## Rationale

- The object backend is RGW or MinIO, not AWS general purpose S3. The
  `x-amz-bucket-namespace` header has no effect there, so sending it would
  suggest behavior the service does not provide.
- Account-regional is not only a uniqueness rule. It changes the visible name
  suffix and adds an API choice. Implementing it touches the resource name, the
  store key, the proto surface, the migration, and every caller. That is larger
  than the follow-up #155 asks for.
- A global namespace refuses a name another project holds, which is stricter
  than the account-regional uniqueness rule. VWS also accepts names AWS
  rejects, because it reserves no account-regional suffix.
- The scan is independent of the namespace choice. The name index makes the
  existing global check cheap without changing what a caller observes.

## Consequences

- Two projects cannot hold the same raw bucket name, and one project cannot
  hold the same raw name in two regions. Account-regional AWS allows the same
  prefix under different account and region suffixes, because the full name
  still differs. VWS reserves no suffix and has no namespace header, so it
  accepts names AWS rejects. This is the tracked parity gap (#170).
- The gRPC API has no namespace field. A caller cannot request an
  account-regional bucket. The S3 front end ignores `x-amz-bucket-namespace`,
  so a request that asks for one creates a global bucket.
- A name is free again after `DeleteBucket` finishes, as before.
- SQLite schema version 3 adds the `bucket_names` index table and fills it from
  existing bucket rows at `Open`.
- Revisit this decision, with a new decision record and an implementation
  issue, when the backend becomes AWS general purpose S3 or a caller needs
  account-regional names. That change must add the region to the bucket key and
  migrate the rows, not only relax the admission check.

## AWS parity notes

- `CreateBucket` in the global namespace: another account's name is 409
  `BucketAlreadyExists`. VWS matches this with `ALREADY_EXISTS` across projects.
- `CreateBucket` in an account-regional namespace: AWS reserves the
  `-{account}-{region}-an` suffix and rejects another account's use of it. VWS
  reserves no suffix, so another project can take a name with someone else's
  suffix. The S3 front end also ignores `x-amz-bucket-namespace`. This is the
  gap (#170).
- Account-regional names must end with `-{12-digit-account-id}-{region}-an`.
  VWS accepts and stores its own resource-id grammar without that suffix.
- `DeleteBucket` frees the name in both AWS namespaces and in VWS.
