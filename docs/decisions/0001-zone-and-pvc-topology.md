# 0001: A volume's zone does not constrain PVC topology (for now)

- Status: accepted
- Date: 2026-10-31
- Issue: #100 (split from #19, item 3)

## Context

An EBS volume lives in one Availability Zone. `CreateVolume` takes a required
`AvailabilityZone`, and `AttachVolume` refuses an instance in another zone with
`InvalidVolume.ZoneMismatch` ("You can only attach an Amazon EBS volume to an
instance if they are in the same Availability Zone", EC2 API error codes).

In vws-storage the zone is part of the volume and its resource name, but the
kube backend does not turn it into scheduling or provisioning constraints:

- `storage.Service` admits a volume only in a configured zone (`-zones`,
  default `vws-1a`; `internal/storage/volume.go`, `unknown zone` is
  `INVALID_ARGUMENT`). Read, update, delete and snapshot requests check the
  zone in the name against the stored one.
- The kube backend writes the zone only as the PVC/DataVolume annotation
  `vws.popododo0720.io/zone` (`internal/backend/kube/kube.go`,
  `datavolume.go`). The claim has no node affinity, no selected node and no
  per-zone StorageClass. A repeat create with another zone is a conflict.
- Each volume type maps to one StorageClass (`Classes`, default `ceph-block`).
  The class is not chosen by zone.
- The vws-deploy Rook bundle (`platform/rook-ceph-cluster.*.values.yaml`) has
  one `ceph-blockpool` (failure domain `host` or `osd`) and one `ceph-block`
  StorageClass with `allowedTopologies: []` and `volumeBindingMode: Immediate`.
  An RBD image in that pool can be mapped on any node of the cluster.
- Nothing in vws-deploy labels nodes with `topology.kubernetes.io/zone` or maps
  a VWS zone to a set of nodes. vws-compute does not place a VM by zone either
  (no node selector or affinity in its Go code).
- vws-compute already enforces the EBS attach rule at the API:
  `checkAttachments` (`internal/compute/controller_instance.go`) refuses a
  volume whose resource name is in another zone than the instance
  (`volume ... is not in the instance zone`).

## Options

1. **Zone constrains topology now.** One StorageClass per zone with
   `allowedTopologies` on `topology.kubernetes.io/zone`, or
   `WaitForFirstConsumer`, and the backend picks the class (or a node
   selector) by zone.
2. **Zone stays logical.** Keep one class per type, `Immediate` binding and
   the zone annotation. Zone parity is enforced at the API layer (storage
   admission and name checks, compute's attach check).

## Decision

Option 2. The kube backend does not add `allowedTopologies`,
`WaitForFirstConsumer` or a selected node for the volume's zone. The zone stays
a validated, stored attribute plus the claim annotation.

## Rationale

- There is no topology to constrain to. The cluster has no zone node labels
  and one Ceph pool reachable from every node, so `allowedTopologies` on a zone
  key would match no node or every node, and `WaitForFirstConsumer` would only
  delay provisioning with no placement gain.
- A per-zone StorageClass would split the type table (`Classes`, #15) by zone
  and need a zone-to-class map the platform does not define. That is a
  platform zone-model decision, not a storage backend one (#19).
- The observable EBS behavior that matters to callers is already there: a
  volume is created only in a known zone, is addressed by its zone, and cannot
  be attached across zones. Topology on the claim would add no API-visible
  behavior today.
- Changing a bound claim's topology later is not possible, but the claim keeps
  the zone annotation, so a future backend can find each volume's zone without
  a migration of stored records.

## Consequences

- No code change and no follow-up implementation issue: the "if yes" branch of
  #100 does not apply.
- Revisit this decision, with a new decision record and an implementation
  issue, when the platform maps a VWS zone to physical failure domains: nodes
  labeled per zone, or a Ceph pool or cluster per zone. At that point a claim
  must be provisioned in its zone (`allowedTopologies` per zone class, or
  `WaitForFirstConsumer` with the VM pinned to the zone), and existing claims
  need a plan because their PVs carry no zone affinity.
- Until then a zone is a logical partition only. Two zones share the same
  physical failure domain, unlike AWS. This is a known parity gap in failure
  isolation, not in the API.

## AWS parity notes

- `CreateVolume` without a known zone: refused (`INVALID_ARGUMENT`); EC2 uses
  `InvalidZone.NotFound` / `InvalidAvailabilityZone`. The wire error mapping
  belongs to the API front end.
- `AttachVolume` across zones: refused by vws-compute as an invalid request.
  EC2 answers `InvalidVolume.ZoneMismatch`. Whether the front end returns that
  exact code is outside this repo and is not checked here.
- Snapshots are regional in EC2 and here (snapshot names and work items carry
  the region). A restore into any configured zone is admitted:
  `admitCreateVolume` does not compare the source volume's zone with the new
  one, which matches EC2 creating a volume from a snapshot in any zone of the
  region. This decision does not change that.
- Failure isolation between zones is not provided (see Consequences).
