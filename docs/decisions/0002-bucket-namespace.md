# 0002: Bucket namespaces: global and account-regional

- Status: accepted, amended 2026-11-02 by #170 (see "Amendment: account-regional
  namespace")
- Date: 2026-10-30
- Issue: #155 (follow-up of #154). Amendment: #170

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
- The amendment below (#170) replaces the earlier plan to add the region to
  the bucket key. The key stays unchanged; see "Representation".

The first two consequences above describe the decision as of 2026-10-30. The
amendment below changes them.

## Amendment: account-regional namespace (#170)

- Date: 2026-11-02
- Sources: official AWS docs only (CreateBucket API reference,
  `x-amz-bucket-namespace`; S3 User Guide "Namespaces for general purpose
  buckets" and "General purpose bucket naming rules"). No real AWS call.

### Decision

VWS supports both namespaces. `global` stays the default. An
`account-regional` bucket is created on request and follows the AWS suffix
rules. Bucket names stay one namespace across projects in the store, as
decided above: an account-regional full name embeds the account and the
region, so full names stay unique across VWS the same way they stay unique
across AWS.

### Representation

- **Account.** The account is the caller's project. A project named `a`
  followed by 12 digits is account `{12 digits}` (the console-api convention
  for an AWS account id). A project of any other form cannot create an
  account-regional bucket.
- **Namespace.** `Bucket.namespace` is an immutable field of the bucket row,
  set at create. In the stored JSON it is `"namespace":"account-regional"`; a
  global bucket writes no field, so its row is byte-identical to a row written
  before #170. The gRPC field is `BucketSpec.namespace`
  (popododo0720/vws-proto#110, `BUCKET_NAMESPACE_GLOBAL` |
  `BUCKET_NAMESPACE_ACCOUNT_REGIONAL`, unspecified means global).
- **Region.** The region is the existing `Bucket.region` row field, and for an
  account-regional bucket also the region segment of the name suffix. Create
  checks that the two agree.
- **Key.** Unchanged: `projects/{project}/buckets/{name}`, and the
  `bucket_names` index keeps one row per full name. Adding the region to the
  key is not needed, because the suffix already makes the full name unique per
  account and region. Keeping the key avoids a row migration and keeps
  `GetBucket`, `ListBuckets` and the backend `bucketID` (a hash of the full
  name) unchanged.
- **Idempotency.** The create fingerprint adds the namespace only for a
  non-global create, so a token from before #170 still matches a global
  retry. A replay with the same token and another namespace is
  `ErrTokenReused`; a new token for a stored name with another namespace is
  `ALREADY_EXISTS`, like a region mismatch.

### Rules at create

The engine checks a new name in `admitCreateBucket` with
`CreateBucketRequest.CheckNamespace`, after the stored-row replay:

- An account-regional name must be a non-empty prefix followed by
  `-{account}-{region}-an`, where account and region are the caller's. The
  suffix counts toward the 63-character limit.
- A name that ends with another account's suffix is refused. AWS: "requests
  to create buckets with another account's suffix will be rejected".
- A global name must not end with `-an`. AWS: only account-regional names can
  end with `-an`.
- A violation is `INVALID_ARGUMENT` with reason `INVALID_BUCKET_NAMESPACE`,
  named after the S3 error `InvalidBucketNamespace`. The AWS docs name no code
  for this case; `InvalidBucketNamespace` is the code the AWS CLI asserts for
  it. The S3 HTTP status and the `x-amz-bucket-namespace` header are the S3
  front end's job (popododo0720/vws-console-api#495).

### Migration

- No SQLite schema change and no row rewrite. The namespace lives in the JSON
  row body, and a row without it is global.
- Existing buckets keep their names and replay as before, because the check
  runs only for a name with no stored row. This includes a global bucket whose
  name ends with `-an` or with another account's suffix.
- Behavior change for new creates: a global create of a name ending with
  `-an` is refused with `INVALID_BUCKET_NAMESPACE`.
- Order: vws-proto release with #110, then storage, then console-api #495.
  Until storage reads `BucketSpec.namespace`, every gRPC create is global.

### Downgrade

- A build before #170 reads an account-regional row as global, because the
  JSON decoder ignores the unknown `namespace` field. The name, region,
  labels and backend data are unchanged, so the bucket stays readable and
  deletable.
- If that build rewrites the row (any bucket mutation), it drops the field.
  After an upgrade again, the bucket reads as global. Its name still carries
  the suffix, so only `namespace` in the response differs. Avoid bucket
  mutations while downgraded, or accept this.
- A create retried across the downgrade with the token of an account-regional
  create gets a different fingerprint and `ErrTokenReused`.
- A downgraded build accepts global `-an` names again.

## AWS parity notes

The notes in this section describe 2026-10-30. After #170 the
account-regional notes are met as described in the amendment, except the
header, which is popododo0720/vws-console-api#495.


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
