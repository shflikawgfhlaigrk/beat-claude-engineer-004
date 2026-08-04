#!/usr/bin/env python3
"""Monthly AWS cost model for the proposed pipeline, with every input labelled.

    python3 cost_model.py                 # steady state
    python3 cost_model.py --spike-days 1  # add one full day at 10x

Prices are AWS us-east-1 public list, on-demand, no Savings Plans or Reserved
Instances, captured 2026-08-03. They are [Benchmarked] in the SCORING.md sense
(named external source), not [Observed]: nobody has run this account yet. Every
volume input is labelled at the point of use. Change a number and the whole
model re-derives, which is the point of shipping it as code rather than prose.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

HOURS = 730  # [Assumed] hours in an average month

# --- demand -----------------------------------------------------------------
EVENTS_PER_DAY = 50_000_000        # [Observed] brief
SPIKE_MULTIPLIER = 10              # [Observed] brief: "10x traffic spikes"
TENANTS = 500                      # [Observed] brief: "500+ customers"
BYTES_PER_EVENT = 230              # [Observed] mean line length, fixtures/event_sample.jsonl
EVENTS_PER_SDK_BATCH = 10          # [Assumed] SDK batches ~10 events per POST; unverified,
                                   #          the SDK cannot be changed so this must be measured
                                   #          at the existing collector before build starts
COMPRESSION_RATIO = 0.11           # [Benchmarked] zstd-3 on JSON event logs, ~9:1 typical
PROFILE_WRITE_FRACTION = 0.10      # [Assumed] share of events that change durable visitor state
DECISION_READ_FRACTION = 0.60      # [Assumed] share of events that trigger a personalization read

# --- unit prices (us-east-1 list, 2026-08-03) [Benchmarked] ------------------
P = {
    "fargate_vcpu_hr": 0.04048,
    "fargate_gb_hr": 0.004445,
    "alb_lcu_hr": 0.008,
    "kinesis_shard_hr": 0.015,
    "kinesis_extended_retention_shard_hr": 0.023,
    "kinesis_efo_consumer_shard_hr": 0.015,
    "kinesis_put_units_per_million": 0.014,
    "kinesis_efo_per_gb": 0.013,
    "lambda_per_million_req": 0.20,
    "lambda_gb_second": 0.0000166667,
    "elasticache_r7g_large_hr": 0.201,
    "dynamodb_ondemand_wru_per_million": 1.25,
    "dynamodb_ondemand_rru_per_million": 0.25,
    "dynamodb_storage_gb_month": 0.25,
    "s3_standard_gb_month": 0.023,
    "s3_put_per_1000": 0.005,
    "athena_per_tb_scanned": 5.00,
    "ec2_m7g_2xlarge_hr": 0.3264,
    "ebs_gp3_gb_month": 0.08,
    "nat_gb": 0.045,
    "cloudwatch_ingest_gb": 0.50,
}


def model(spike_days: float = 0.0):
    """Return line items for a month with `spike_days` days at 10x volume."""
    base_days = 30 - spike_days
    events_month = EVENTS_PER_DAY * (base_days + spike_days * SPIKE_MULTIPLIER)
    eps_avg = events_month / (30 * 86400)
    eps_spike = (EVENTS_PER_DAY * SPIKE_MULTIPLIER) / 86400
    gb_month_raw = events_month * BYTES_PER_EVENT / 1e9
    gb_month_compressed = gb_month_raw * COMPRESSION_RATIO
    items = []

    def add(component, choice, usd, note):
        items.append({"component": component, "choice": choice,
                      "usd_month": round(usd, 2), "note": note})

    # Collector: stateless HTTP -> durable enqueue. Sized for the spike, not the
    # average, because the spike is the failure mode we are being paid to fix.
    # [Estimated] 2,000 events/sec/task: the CPU floor is 112k/s/core
    # (see out/bench_ingest.json), so the binding limit is HTTP + Kinesis PUT,
    # not the transform. 2,000/s/task is a deliberately pessimistic 50x discount.
    tasks = max(6, int(eps_spike / 2000) + 2)  # min 6 for 3-AZ x 2
    fargate = tasks * (1.0 * P["fargate_vcpu_hr"] + 2.0 * P["fargate_gb_hr"]) * HOURS
    add("Collector (ECS Fargate)", "{} tasks x 1 vCPU / 2 GB".format(tasks), fargate,
        "[Estimated] sized for 10x spike held continuously, not autoscaled into it")

    # ALB LCUs on the existing listener (the SDK endpoint cannot move).
    req_per_sec = eps_avg / EVENTS_PER_SDK_BATCH
    lcu = max(req_per_sec / 25.0, (eps_avg * BYTES_PER_EVENT / 1e6) / 1.0)
    add("ALB", "{:.1f} LCU".format(lcu), lcu * P["alb_lcu_hr"] * HOURS,
        "[Estimated] new-connection-bound; existing listener reused, no SDK change")

    # Kinesis Data Streams, provisioned. Chosen over on-demand deliberately:
    # on-demand doubles capacity per 15 minutes, which does not survive a step
    # function from 1x to 10x at 00:00 on Black Friday.
    shards = max(4, int(eps_spike / 900) + 1)  # 1000 rec/s/shard, 10% headroom
    ks = shards * P["kinesis_shard_hr"] * HOURS
    ks += shards * P["kinesis_extended_retention_shard_hr"] * HOURS  # 7-day replay
    ks += (events_month / 1e6) * P["kinesis_put_units_per_million"]
    consumers = 3  # serving store, lake writer, decisioning
    ks += consumers * shards * P["kinesis_efo_consumer_shard_hr"] * HOURS
    ks += consumers * gb_month_raw * P["kinesis_efo_per_gb"]
    add("Kinesis Data Streams", "{} shards provisioned, 7d retention, {} EFO consumers"
        .format(shards, consumers), ks,
        "[Estimated] shard count from 10x spike / 900 rec/s; 7d retention is the replay budget")

    # Consumers.
    batch = 500
    invocations = events_month / batch * consumers
    lam = invocations / 1e6 * P["lambda_per_million_req"]
    lam += invocations * 0.25 * 1.0 * P["lambda_gb_second"]  # [Assumed] 250ms @ 1GB
    add("Stream consumers (Lambda)", "{:.0f}M invocations, 250ms @ 1 GB"
        .format(invocations / 1e6), lam,
        "[Assumed] 250ms/batch dominated by downstream writes, not CPU")

    # Idempotency + hot visitor state. Redis rather than DynamoDB for the
    # 24h dedupe set: 1.5B conditional writes/month on DynamoDB on-demand is
    # ~$1,875/mo of pure duplicate-suppression, for state that is disposable.
    redis_nodes = 6  # 3 shards x 1 replica
    add("ElastiCache Redis", "{} x r7g.large (dedupe set + hot counters)".format(redis_nodes),
        redis_nodes * P["elasticache_r7g_large_hr"] * HOURS,
        "[Estimated] 24h event_id dedupe set + segment counters; disposable state")

    # Durable visitor profile for personalization point reads.
    wru = events_month * PROFILE_WRITE_FRACTION / 1e6
    rru = events_month * DECISION_READ_FRACTION * 0.5 / 1e6  # eventually consistent
    ddb = wru * P["dynamodb_ondemand_wru_per_million"] + rru * P["dynamodb_ondemand_rru_per_million"]
    ddb += 250 * P["dynamodb_storage_gb_month"]  # [Assumed] 250 GB of visitor profiles
    add("DynamoDB (visitor profiles)", "{:.0f}M WRU / {:.0f}M RRU on-demand".format(wru, rru), ddb,
        "[Assumed] 10% of events change durable state; 60% trigger a decision read")

    # Serving store for dashboards.
    ch_nodes = 3
    ch = ch_nodes * P["ec2_m7g_2xlarge_hr"] * HOURS + ch_nodes * 2000 * P["ebs_gp3_gb_month"]
    add("ClickHouse (self-managed)", "{} x m7g.2xlarge + 2 TB gp3 each".format(ch_nodes), ch,
        "[Estimated] MVP alternative is ClickHouse Cloud at roughly 2-3x this, "
        "buying back the ops hours of 2 engineers - see ANSWER.md trade-offs")

    # Lake: Iceberg on S3. Row-level deletes are why Iceberg, not bare Parquet.
    s3_gb_year1 = gb_month_compressed * 6  # [Assumed] 6-month average holding in year 1
    puts = TENANTS * 24 * 30 * 2  # hourly partition files, 2 tables
    s3 = s3_gb_year1 * P["s3_standard_gb_month"] + puts / 1000 * P["s3_put_per_1000"]
    add("S3 + Iceberg lake", "{:.0f} GB compressed, hourly per-tenant partitions"
        .format(s3_gb_year1), s3,
        "[Estimated] {:.1f} GB/day raw at {:.0f}:1 compression".format(
            gb_month_raw / 30, 1 / COMPRESSION_RATIO))

    # Warehouse export jobs.
    add("Athena (export + reconciliation)", "[Assumed] 60 TB scanned/month",
        60 * P["athena_per_tb_scanned"],
        "[Assumed] daily per-tenant export + hourly old-vs-new reconciliation queries")

    # Network + observability. The trap here is logging 1.5B events at $0.50/GB.
    add("NAT + inter-AZ transfer", "[Assumed] 3 TB/month", 3000 * P["nat_gb"],
        "[Assumed] cross-AZ replication and egress to customer warehouses")
    add("CloudWatch (sampled)", "[Assumed] 400 GB/month logs + metrics",
        400 * P["cloudwatch_ingest_gb"],
        "[Estimated] per-event logging would be ~5.7 TB/mo (~$2,850); logs are "
        "sampled at 1% with quarantine rows logged at 100%")

    subtotal = sum(i["usd_month"] for i in items)
    contingency = subtotal * 0.30  # [Assumed] 30% for what the model missed
    add("Contingency", "30% of modelled subtotal", contingency,
        "[Assumed] unmodelled: KMS, Secrets Manager, ECR, WAF, backups, dev/stage envs")

    total = subtotal + contingency
    return {
        "spike_days": spike_days,
        "events_month": int(events_month),
        "events_per_sec_avg": round(eps_avg, 1),
        "events_per_sec_at_10x": round(eps_spike, 1),
        "raw_gb_per_day": round(gb_month_raw / 30, 2),
        "items": items,
        "subtotal_usd_month": round(subtotal, 2),
        "total_usd_month": round(total, 2),
        "ceiling_usd_month": 50000,          # [Observed] brief
        "headroom_usd_month": round(50000 - total, 2),
        "pct_of_ceiling": round(100 * total / 50000, 1),
    }


def render(r):
    print("events/month      : {:,}  ({} day(s) at {}x)".format(
        r["events_month"], r["spike_days"], SPIKE_MULTIPLIER))
    print("events/sec average: {:,.1f}   at 10x spike: {:,.1f}".format(
        r["events_per_sec_avg"], r["events_per_sec_at_10x"]))
    print("raw volume        : {} GB/day\n".format(r["raw_gb_per_day"]))
    print("{:<32} {:<46} {:>10}".format("COMPONENT", "CHOICE", "USD/MONTH"))
    print("-" * 90)
    for i in r["items"]:
        print("{:<32} {:<46} {:>10,.0f}".format(i["component"], i["choice"][:46], i["usd_month"]))
        print("{:<32} {}".format("", i["note"]))
    print("-" * 90)
    print("{:<79} {:>10,.0f}".format("TOTAL [Estimated]", r["total_usd_month"]))
    print("{:<79} {:>10,.0f}".format("CEILING [Observed, brief]", r["ceiling_usd_month"]))
    print("{:<79} {:>10,.0f}".format("HEADROOM", r["headroom_usd_month"]))
    print("\n=> {}% of the stated ceiling. The budget is not the binding constraint; "
          "the two engineers are.".format(r["pct_of_ceiling"]))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--spike-days", type=float, default=0.0)
    ap.add_argument("--json", dest="json_out")
    a = ap.parse_args()
    res = model(a.spike_days)
    render(res)
    if a.json_out:
        Path(a.json_out).parent.mkdir(parents=True, exist_ok=True)
        Path(a.json_out).write_text(json.dumps(res, indent=2) + "\n")
        print("\nwrote {}".format(a.json_out))
