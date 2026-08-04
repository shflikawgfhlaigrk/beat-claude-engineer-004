#!/usr/bin/env python3
"""Run the ingest-edge contract over the challenge fixture and report.

    python3 detect_anomalies.py                     # human-readable report
    python3 detect_anomalies.py --json out/anomaly_report.json

Exit code is 1 if any line quarantines, which is the behaviour the CI gate in
the migration plan uses: a quarantine is a defect with an owner, not weather.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from pipeline.normalize import (  # noqa: E402
    ACCEPT, ACCEPT_FLAGGED, DUPLICATE, QUARANTINE, A_BOT_BURST, A_CLIENT_AGGREGATE,
    A_LATE_IDENTITY, A_PRIVACY_REQUEST, normalize_batch,
)

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "event_sample.jsonl"


def _ts(value):
    if not value:
        return None
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def cross_event_analysis(verdicts):
    """Findings that only exist across events, not inside one."""
    live = [v for v in verdicts if v.canonical and v.verdict in (ACCEPT, ACCEPT_FLAGGED)]
    findings = {}

    # 1. Retroactive identity stitching: events that predate the identify call
    #    for the same visitor. These are the events a naive pipeline attributes
    #    to nobody and never repairs.
    identifies = {}
    for v in live:
        c = v.canonical
        if c["type"] == "identify" and c.get("user_id"):
            identifies[(c["tenant_id"], c["anonymous_id"])] = (c["user_id"], _ts(c["ts"]), v.event_id)
    stitch = []
    for (tenant, anon), (user_id, at, ident_event) in sorted(identifies.items()):
        prior = [v.event_id for v in live
                 if v.canonical["tenant_id"] == tenant
                 and v.canonical["anonymous_id"] == anon
                 and _ts(v.canonical["ts"]) and _ts(v.canonical["ts"]) < at]
        if prior:
            stitch.append({"tenant_id": tenant, "anonymous_id": anon, "user_id": user_id,
                           "identify_event": ident_event, "backfill_events": sorted(prior),
                           "anomaly": A_LATE_IDENTITY})
    findings["retroactive_identity_stitch"] = stitch

    # 2. Identity that arrived out of band: user_id present with no identify
    #    event in the window. Proves the stitch table cannot be rebuilt from a
    #    single window of the stream; it needs durable per-visitor state.
    oob = sorted({(v.canonical["tenant_id"], v.canonical["anonymous_id"], v.canonical["user_id"])
                  for v in live if v.canonical.get("user_id")
                  and (v.canonical["tenant_id"], v.canonical["anonymous_id"]) not in identifies})
    findings["identity_without_identify_event"] = [
        {"tenant_id": t, "anonymous_id": a, "user_id": u,
         "event_ids": sorted(v.event_id for v in live
                             if v.canonical["anonymous_id"] == a and v.canonical["user_id"] == u)}
        for t, a, u in oob]

    # 3. GDPR deletion blast radius. A delete request names a user_id, but the
    #    subject's data is also keyed by anonymous_id, and some of it is sitting
    #    in quarantine, which is exactly where deletion jobs forget to look.
    cascades = []
    for v in verdicts:
        if A_PRIVACY_REQUEST not in v.anomalies:
            continue
        c = v.canonical
        anon, user = c["anonymous_id"], c["user_id"]
        by_user = sorted({x.event_id for x in verdicts
                          if x.canonical and x.canonical.get("user_id") == user and user})
        by_anon = sorted({x.event_id for x in verdicts
                          if x.canonical and x.canonical.get("anonymous_id") == anon})
        quarantined = sorted({x.event_id for x in verdicts if x.verdict == QUARANTINE
                              and x.canonical and (x.canonical.get("anonymous_id") == anon
                                                   or x.canonical.get("user_id") == user)})
        cascades.append({
            "request_event": v.event_id, "tenant_id": c["tenant_id"],
            "regulation": c["properties"].get("regulation"),
            "subject_user_id": user, "subject_anonymous_id": anon,
            "matched_by_user_id": by_user,
            "matched_only_by_anonymous_id": sorted(set(by_anon) - set(by_user)),
            "matched_in_quarantine": quarantined,
        })
    findings["deletion_cascade"] = cascades

    # 4. Client-computed aggregates vs what the server can actually observe.
    #    This is the one that would fire a personalization campaign on a number
    #    nobody can reproduce.
    contradictions = []
    for v in verdicts:
        if A_CLIENT_AGGREGATE not in v.anomalies or not v.canonical:
            continue
        c = v.canonical
        claimed = {k: c["properties"][k] for k in v.detail.get("client_aggregates", [])}
        observed_pricing = sorted(x.event_id for x in live
                                  if x.canonical["anonymous_id"] == c["anonymous_id"]
                                  and x.canonical["type"] == "page_view"
                                  and x.canonical["properties"].get("path") == "/pricing")
        contradictions.append({
            "event_id": v.event_id, "claimed": claimed,
            "server_observable_pricing_views_in_sample": len(observed_pricing),
            "server_observable_event_ids": observed_pricing,
        })
    findings["client_aggregate_vs_server_truth"] = contradictions

    # 5. Traffic-mix impact of the bot filter, per tenant.
    per_tenant = defaultdict(lambda: {"events": 0, "bot_flagged": 0})
    for v in live:
        per_tenant[v.tenant_id]["events"] += 1
        if A_BOT_BURST in v.anomalies:
            per_tenant[v.tenant_id]["bot_flagged"] += 1
    findings["bot_share_by_tenant"] = {
        t: dict(d, bot_share_pct=round(100.0 * d["bot_flagged"] / d["events"], 1))
        for t, d in sorted(per_tenant.items()) if d["events"]}

    return findings


def build_report(path: Path):
    raw = path.read_bytes()
    lines = raw.decode("utf-8").splitlines()
    verdicts = normalize_batch(lines)
    counts = Counter(v.verdict for v in verdicts)
    anomaly_counts = Counter(a for v in verdicts for a in set(v.anomalies))
    return {
        "fixture": str(path.name),
        "fixture_sha256": hashlib.sha256(raw).hexdigest(),
        "lines_read": len([ln for ln in lines if ln.strip()]),
        "verdict_counts": dict(sorted(counts.items())),
        "anomaly_counts": dict(sorted(anomaly_counts.items())),
        "verdicts": [v.as_dict() for v in verdicts],
        "cross_event": cross_event_analysis(verdicts),
    }


def print_report(rep):
    w = sys.stdout.write
    w("fixture         : {}\n".format(rep["fixture"]))
    w("sha256          : {}\n".format(rep["fixture_sha256"]))
    w("lines read      : {}\n".format(rep["lines_read"]))
    w("verdicts        : {}\n".format(", ".join("{}={}".format(k, v)
                                                for k, v in rep["verdict_counts"].items())))
    w("\nANOMALY CLASSES\n")
    for cls, n in rep["anomaly_counts"].items():
        ids = [v["event_id"] or "line-{}".format(v["line_no"])
               for v in rep["verdicts"] if cls in v["anomalies"]]
        w("  {:<32} n={}  {}\n".format(cls, n, ", ".join(ids)))

    w("\nPER-LINE VERDICTS\n")
    for v in rep["verdicts"]:
        if v["verdict"] == ACCEPT:
            continue
        w("  line {:>2}  {:<15} {:<10} {}\n".format(
            v["line_no"], v["event_id"] or "-", v["verdict"], ",".join(v["anomalies"])))
        for k, val in v["detail"].items():
            w("           {}: {}\n".format(k, val))

    ce = rep["cross_event"]
    w("\nCROSS-EVENT FINDINGS\n")
    for s in ce["retroactive_identity_stitch"]:
        w("  stitch  {} -> {} via {}: backfill {}\n".format(
            s["anonymous_id"], s["user_id"], s["identify_event"], ", ".join(s["backfill_events"])))
    for s in ce["identity_without_identify_event"]:
        w("  no-identify  {} carries {} on {} (identity arrived outside this window)\n".format(
            s["anonymous_id"], s["user_id"], ", ".join(s["event_ids"])))
    for c in ce["deletion_cascade"]:
        w("  delete  {} ({}, tenant {}): by user_id {} | ONLY by anonymous_id {} | in quarantine {}\n"
          .format(c["request_event"], c["regulation"], c["tenant_id"],
                  ", ".join(c["matched_by_user_id"]) or "-",
                  ", ".join(c["matched_only_by_anonymous_id"]) or "-",
                  ", ".join(c["matched_in_quarantine"]) or "-"))
    for c in ce["client_aggregate_vs_server_truth"]:
        w("  aggregate  {} claims {} ; server can observe {} /pricing view(s) {}\n".format(
            c["event_id"], c["claimed"], c["server_observable_pricing_views_in_sample"],
            c["server_observable_event_ids"]))
    for t, d in ce["bot_share_by_tenant"].items():
        w("  bot-share  tenant {}: {}/{} events flagged non-human ({}%)\n".format(
            t, d["bot_flagged"], d["events"], d["bot_share_pct"]))
    w("\nn=25 lines. These are counts from one synthetic sample, not rates. "
      "Nothing here is extrapolated to 50M/day.\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("fixture", nargs="?", default=str(FIXTURE))
    ap.add_argument("--json", dest="json_out")
    args = ap.parse_args()
    rep = build_report(Path(args.fixture))
    print_report(rep)
    if args.json_out:
        Path(args.json_out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.json_out).write_text(json.dumps(rep, indent=2, sort_keys=True) + "\n")
        print("\nwrote {}".format(args.json_out))
    return 1 if rep["verdict_counts"].get(QUARANTINE) else 0


if __name__ == "__main__":
    sys.exit(main())
