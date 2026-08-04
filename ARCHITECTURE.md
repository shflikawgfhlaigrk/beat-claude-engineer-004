# Architecture Diagrams — Engineer 004

Companion to `ANSWER.md`. Diagrams do not count toward the 4-page limit.

## 1. Data flow, SDK to dashboard

```mermaid
flowchart LR
  SDK["Existing JS SDK<br/>(UNCHANGED - constraint)"]
  subgraph EDGE["Edge - same hostname, same path"]
    ALB["ALB (existing listener)"]
    COL["Collector (Fargate)<br/>authn, tenant resolve,<br/>quota, durable enqueue<br/>200 only after PUT ack"]
    SPILL[("Local disk spill<br/>+ SQS fallback")]
  end
  subgraph BUS["Durable log"]
    KDS["Kinesis Data Streams<br/>7 shards, 7-day retention<br/>partition key = tenant_id:anonymous_id"]
  end
  subgraph NORM["Ingest-edge contract (pipeline/normalize.py)"]
    N["normalize + classify<br/>alias translation, PII detect,<br/>clock check, bot flag,<br/>idempotency on event_id"]
    Q[("Quarantine table<br/>raw bytes retained,<br/>tenant-addressable")]
  end
  LEDGER[("Accept ledger<br/>event_id + tenant + minute<br/>= reconciliation ground truth")]
  subgraph SERVE["Serving"]
    CH[("ClickHouse<br/>ReplacingMergeTree(event_id)<br/>dashboards, segments")]
    RED[("ElastiCache Redis<br/>24h dedupe set,<br/>hot segment counters")]
    DDB[("DynamoDB<br/>visitor profile,<br/>point read for decisioning")]
  end
  subgraph LAKE["Lake + export"]
    ICE[("S3 + Apache Iceberg<br/>partition: tenant_id / hour<br/>row-level deletes")]
    EXP["Per-tenant export<br/>Snowflake / BigQuery"]
  end
  DASH["Real-time dashboard"]
  DEC["Personalization decision API"]

  SDK -->|"HTTPS batch POST"| ALB --> COL
  COL -.->|"PUT fails"| SPILL -.-> KDS
  COL --> KDS
  COL --> LEDGER
  KDS --> N
  N --> Q
  N --> CH
  N --> RED
  N --> DDB
  N --> ICE
  ICE --> EXP
  CH --> DASH
  DDB --> DEC
  RED --> DEC
  LEDGER -.->|"count reconciliation"| CH
  Q -.->|"replay after fix"| KDS
```

## 2. Verdict machine — what happens to one line

Every input line leaves with exactly one verdict. There is no fifth outcome,
and there is no path that decrements a counter without writing a row. This is
enforced by `tests/test_normalize.py::TestNoSilentLoss`.

```mermaid
flowchart TD
  IN["raw line"] --> P{"parses as JSON object?"}
  P -->|no| QM["QUARANTINE: malformed_json<br/>raw bytes kept, event_id regex-scraped<br/>evt-0020"]
  P -->|yes| A["alias translation<br/>timestamp->ts, page_path->path,<br/>pageview->page_view<br/>evt-0009"]
  A --> T{"tenant_id present?"}
  T -->|no| QT["QUARANTINE: missing_tenant<br/>unroutable, unbillable, undeletable<br/>evt-0011"]
  T -->|yes| D{"(tenant, event_id) seen<br/>in 24h dedupe set?"}
  D -->|yes| DUP["DUPLICATE - counted, not stored<br/>evt-0002"]
  D -->|no| F["flag: clock skew, future ts,<br/>PII, bot burst, client aggregate,<br/>privacy request"]
  F --> FL{"any flag?"}
  FL -->|yes| AF["ACCEPT_FLAGGED<br/>stored with flags; flags are<br/>query-time filters, not deletions"]
  FL -->|no| AC["ACCEPT"]
```

## 3. Migration — parallel run and rollback

Old pipeline keeps writing throughout. Rollback is a read-path flag flip, not a
data movement, which is why it can be automatic.

```mermaid
flowchart TD
  SDK["SDK (unchanged)"] --> COL["Collector"]
  COL --> OLD["Existing pipeline<br/>(untouched, still authoritative)"]
  COL --> NEW["New pipeline<br/>(shadow)"]
  OLD --> ODB[("Current store")]
  NEW --> NDB[("ClickHouse")]
  ODB --> REC["Hourly reconciler<br/>count by tenant/hour,<br/>unique visitors,<br/>conversion events,<br/>10k sampled event_ids"]
  NDB --> REC
  REC --> GATE{"per-tenant gate<br/>|delta| <= 0.1% for 14 days<br/>AND 100% of sampled ids in both"}
  GATE -->|pass| FLAG["Flip read flag for that tenant"]
  GATE -->|fail| HOLD["Tenant stays on old store<br/>defect gets an owner"]
  FLAG --> WATCH{"auto-rollback triggers<br/>divergence > 0.5% / 15 min<br/>OR p99 e2e > 10s / 10 min<br/>OR quarantine rate > 0.5%"}
  WATCH -->|fire| RB["Flag reverts, page on-call<br/>RTO = flag propagation only"]
```

## 4. GDPR deletion blast radius (evt-0017)

The request names `u-1077`. The subject's other event, `evt-0006`, carries no
`user_id` — it is reachable only through `anon-77a`. A deletion job written
against `user_id` alone leaves it behind, and quarantine rows are the second
place deletion jobs forget to look.

```mermaid
flowchart LR
  REQ["evt-0017<br/>privacy_request delete_all_data<br/>GDPR, tenant t-088<br/>user_id u-1077, anon-77a"]
  REQ --> R1["1. Resolve subject keys<br/>u-1077 + every anonymous_id<br/>ever stitched to it"]
  R1 --> R2["2. Destroy per-subject data key<br/>(crypto-shred: PII columns<br/>become unreadable in every<br/>copy, including backups)"]
  R2 --> R3["3. Iceberg row-level delete<br/>by subject key"]
  R3 --> R4["4. Delete DynamoDB profile<br/>+ Redis counters"]
  R4 --> R5["5. Sweep QUARANTINE table<br/>evt-0011-class rows have<br/>no tenant: swept by<br/>anonymous_id and raw scan"]
  R5 --> R6["6. Emit tombstone to Kinesis<br/>so downstream warehouse<br/>exports replay the delete"]
  R6 --> R7["7. HUMAN sign-off before<br/>the destructive step runs<br/>+ signed completion record"]
```
