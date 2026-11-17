# Schema v1
SQLAlchemy metadata creates users, audits, datasets, plans, traces. Natural names are unique; foreign keys reference parent entities. Each trace stores immutable JSON input snapshots. PostgreSQL persists metadata and teaching datasets; the bounded Python operators execute the plan.

# Schema v2 (additive, applied at startup by src/config/schema.py)
`traces.status` gains `pending` (accepted, delivery not yet confirmed) alongside `queued/running/completed/failed`. New nullable/defaulted columns keep existing rows valid:

| column | purpose |
|---|---|
| attempt | monotonic claim counter; fencing token paired with owner |
| recoveries | number of automatic takeovers, shown in the UI |
| owner | `host:pid:uuid` of the lease holder |
| lease_expires_at | heartbeat deadline; past it the row may be taken over |
| heartbeat_at | last successful heartbeat |
| dispatched_at | last Redis enqueue; drives re-delivery of lost messages |
| started_at | first successful claim (preserved across takeovers) |
| completed_at | terminal timestamp for completed/failed |
| updated_at | last lifecycle write |

Existing `running` rows from v1 keep their data; after upgrade the sweeper treats them as expired leases and re-queues the same records.
