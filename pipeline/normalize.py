"""Ingest-edge contract for the real-time analytics pipeline.

This is the riskiest component in the design, so it is the one that exists as
running code rather than as a box on a diagram: everything the pipeline
promises about loss, duplication, ordering, tenancy and PII is decided here,
in the first 200 microseconds after an event lands.

Design rule enforced by this module: **nothing is ever dropped.** Every input
line leaves as exactly one Verdict with a class, and every Verdict is
accountable. A line that cannot be parsed becomes a QUARANTINE verdict that
retains the raw bytes; it does not become a silent decrement in a counter.
That is the difference between "3% event loss" and "3% of events are in the
quarantine table, here they are."

Pure stdlib, no I/O, no clock reads outside of what is passed in: the module
is a function of (raw_lines, batch_context) so it can be unit-tested and
benchmarked deterministically.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

SCHEMA_VERSION = "canonical-v1"

# ---------------------------------------------------------------------------
# Verdict classes. ACCEPT and ACCEPT_FLAGGED reach the serving store;
# QUARANTINE reaches the quarantine table and the operator dashboard; DUPLICATE
# is suppressed downstream but still counted. No other outcomes exist.
# ---------------------------------------------------------------------------
ACCEPT = "accept"
ACCEPT_FLAGGED = "accept_flagged"
QUARANTINE = "quarantine"
DUPLICATE = "duplicate"

# Anomaly classes. These are the taxonomy the operator dashboard is built on;
# each one has a defined owner and a defined action.
A_MALFORMED = "malformed_json"
A_SCHEMA_DRIFT = "schema_drift"
A_MISSING_TENANT = "missing_tenant"
A_DUPLICATE_ID = "duplicate_event_id"
A_NEGATIVE_LATENCY = "negative_transit_latency"
A_FUTURE_TS = "implausible_future_timestamp"
A_PII_PAYLOAD = "unclassified_pii_in_properties"
A_BOT_BURST = "non_human_burst"
A_CLIENT_AGGREGATE = "client_computed_aggregate"
A_PRIVACY_REQUEST = "privacy_request"
A_LATE_IDENTITY = "retroactive_identity_stitch"

# ---------------------------------------------------------------------------
# Tunables. Every one of these is a policy decision, not a constant: they are
# named here so the values are reviewable and so changing one is a code review,
# not a config drive-by. Sources are labelled in ANSWER.md.
# ---------------------------------------------------------------------------
MAX_CLOCK_SKEW_S = 300.0          # [Assumed] tolerance for consumer-device clock error
FUTURE_TS_HARD_LIMIT_S = 86400.0  # [Assumed] beyond one day ahead the ts is unusable
BURST_WINDOW_S = 1.0              # [Assumed] window for the non-human burst heuristic
BURST_MIN_EVENTS = 4              # [Assumed] distinct page_views in window to flag
KNOWN_TYPES = {"page_view", "click", "form_submit", "custom", "identify", "privacy_request"}

# Legacy SDK field spellings still in the wild. Cannot be fixed by shipping a
# new SDK (brief constraint), so it is fixed here, permanently, by translation.
FIELD_ALIASES = {"timestamp": "ts", "sent_at": "ts", "event": "type", "name": "type"}
PROPERTY_ALIASES = {"page_path": "path", "url_path": "path", "ref": "referrer", "referer": "referrer"}
TYPE_ALIASES = {"pageview": "page_view", "page-view": "page_view", "pageView": "page_view",
                "formsubmit": "form_submit", "form-submit": "form_submit"}

# PII detectors. Deliberately narrow and high-precision: a false positive here
# encrypts a field nobody needed encrypted (cheap); a false negative writes a
# customer's prospect's phone number into 500 tenants' shared analytics store
# in plaintext (not cheap).
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
PHONE_RE = re.compile(r"(?:\+?\d[\d\-\.\(\) ]{7,}\d)")
# NOTE: a bare "name" hint was in this list on the first pass and produced two
# false positives on the fixture (evt-0016 `properties.name="report_exported"`,
# evt-0019 `properties.name="viewed_pricing"` - both are custom *event* names,
# not people). Kept as a comment because it is the exact shape of the mistake a
# key-name PII scanner makes at scale, and the fix is to require a person-ish
# qualifier rather than to trust the word "name".
PII_KEY_HINTS = ("email", "phone", "mobile", "ssn", "dob", "birth", "street",
                 "postcode", "postal", "zip", "ip_addr", "card", "passport",
                 "first_name", "last_name", "full_name", "given_name",
                 "surname", "customer_name", "contact_name")

# Property keys whose value is an aggregate the *client* computed. The server
# cannot reproduce them, so they are never allowed to drive a segment.
CLIENT_AGGREGATE_KEYS = ("count_", "_count", "total_", "_total", "session_number",
                         "visit_number", "times_", "_seen")

BOT_REFERRER_HINTS = ("bot", "crawler", "spider", "scanner", "monitor", "uptime", "headless")


@dataclass
class Verdict:
    """One input line's disposition. Serializable; this is the quarantine row."""
    line_no: int
    verdict: str
    event_id: Optional[str] = None
    tenant_id: Optional[str] = None
    anomalies: List[str] = field(default_factory=list)
    detail: Dict[str, Any] = field(default_factory=dict)
    canonical: Optional[Dict[str, Any]] = None
    raw: Optional[str] = None

    def as_dict(self) -> Dict[str, Any]:
        d = {"line_no": self.line_no, "verdict": self.verdict, "event_id": self.event_id,
             "tenant_id": self.tenant_id, "anomalies": sorted(self.anomalies),
             "detail": self.detail}
        if self.verdict == QUARANTINE:
            d["raw"] = self.raw
        return d


def _parse_ts(value: Any) -> Optional[datetime]:
    if not isinstance(value, str):
        return None
    v = value.strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(v)
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _walk_strings(obj: Any, path: str = "") -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            out.extend(_walk_strings(v, "{}.{}".format(path, k) if path else str(k)))
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            out.extend(_walk_strings(v, "{}[{}]".format(path, i)))
    elif isinstance(obj, str):
        out.append((path, obj))
    return out


def detect_pii(properties: Dict[str, Any]) -> List[str]:
    """Return dotted paths of property fields that look like personal data."""
    hits = []
    for path, value in _walk_strings(properties):
        leaf = path.split(".")[-1].lower()
        if any(h in leaf for h in PII_KEY_HINTS) or EMAIL_RE.search(value) or PHONE_RE.search(value):
            hits.append(path)
    return sorted(set(hits))


def detect_client_aggregates(properties: Dict[str, Any]) -> List[str]:
    """Return property keys that carry a client-computed count."""
    hits = []
    for k, v in properties.items():
        kl = k.lower()
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            if any(tok in kl for tok in CLIENT_AGGREGATE_KEYS):
                hits.append(k)
    return sorted(hits)


def normalize_line(line_no: int, raw: str) -> Verdict:
    """Parse and canonicalize one raw line. Never raises, never drops."""
    raw = raw.rstrip("\n")
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError as exc:
        # Best-effort id extraction so a quarantined line is still traceable and
        # still deletable under a GDPR request.
        m = re.search(r'"event_id"\s*:\s*"([^"]+)"', raw)
        t = re.search(r'"tenant_id"\s*:\s*"([^"]+)"', raw)
        return Verdict(line_no, QUARANTINE, m.group(1) if m else None,
                       t.group(1) if t else None, [A_MALFORMED],
                       {"parse_error": "{} at col {}".format(exc.msg, exc.colno)}, raw=raw)
    if not isinstance(obj, dict):
        return Verdict(line_no, QUARANTINE, None, None, [A_MALFORMED],
                       {"parse_error": "top-level value is not an object"}, raw=raw)

    anomalies: List[str] = []
    detail: Dict[str, Any] = {}

    # --- field-name drift -------------------------------------------------
    drift: List[str] = []
    for legacy, canonical_name in FIELD_ALIASES.items():
        if legacy in obj and canonical_name not in obj:
            obj[canonical_name] = obj.pop(legacy)
            drift.append("{}->{}".format(legacy, canonical_name))

    ev_type = obj.get("type")
    if isinstance(ev_type, str) and ev_type in TYPE_ALIASES:
        drift.append("type:{}->{}".format(ev_type, TYPE_ALIASES[ev_type]))
        ev_type = TYPE_ALIASES[ev_type]

    props = obj.get("properties")
    props = dict(props) if isinstance(props, dict) else {}
    for legacy, canonical_name in PROPERTY_ALIASES.items():
        if legacy in props and canonical_name not in props:
            props[canonical_name] = props.pop(legacy)
            drift.append("properties.{}->{}".format(legacy, canonical_name))

    if "received_at" not in obj:
        drift.append("received_at:absent")

    if drift:
        anomalies.append(A_SCHEMA_DRIFT)
        detail["schema_drift"] = sorted(drift)

    if isinstance(ev_type, str) and ev_type not in KNOWN_TYPES:
        anomalies.append(A_SCHEMA_DRIFT)
        detail.setdefault("schema_drift", []).append("unknown_type:{}".format(ev_type))

    # --- tenancy ----------------------------------------------------------
    tenant_id = obj.get("tenant_id")
    if not tenant_id:
        anomalies.append(A_MISSING_TENANT)
        detail["tenant"] = "null tenant_id: unroutable, cannot be billed, stored, or deleted per-tenant"

    # --- time -------------------------------------------------------------
    ts = _parse_ts(obj.get("ts"))
    received_at = _parse_ts(obj.get("received_at"))
    if ts is None:
        anomalies.append(A_SCHEMA_DRIFT)
        detail.setdefault("schema_drift", []).append("ts:unparseable")
    if ts and received_at:
        transit = (received_at - ts).total_seconds()
        detail["transit_latency_s"] = round(transit, 3)
        if transit < 0:
            anomalies.append(A_NEGATIVE_LATENCY)
            if abs(transit) > FUTURE_TS_HARD_LIMIT_S:
                anomalies.append(A_FUTURE_TS)
            detail["clock"] = ("event ts is {:.1f}s ahead of server receipt"
                               .format(abs(transit)))

    # --- payload governance ----------------------------------------------
    pii = detect_pii(props)
    if pii:
        anomalies.append(A_PII_PAYLOAD)
        detail["pii_fields"] = pii

    aggregates = detect_client_aggregates(props)
    if aggregates:
        anomalies.append(A_CLIENT_AGGREGATE)
        detail["client_aggregates"] = aggregates

    referrer = str(props.get("referrer") or "")
    if any(h in referrer.lower() for h in BOT_REFERRER_HINTS):
        anomalies.append(A_BOT_BURST)
        detail["bot_signal"] = "referrer host matches non-human hint list"

    if ev_type == "privacy_request":
        anomalies.append(A_PRIVACY_REQUEST)
        detail["privacy_request"] = {
            "request": props.get("request"), "regulation": props.get("regulation"),
            "subject_user_id": obj.get("user_id"), "subject_anonymous_id": obj.get("anonymous_id"),
        }

    canonical = {
        "schema_version": SCHEMA_VERSION,
        "event_id": obj.get("event_id"),
        "tenant_id": tenant_id,
        "anonymous_id": obj.get("anonymous_id"),
        "user_id": obj.get("user_id"),
        "type": ev_type,
        "ts": ts.isoformat() if ts else None,
        "received_at": received_at.isoformat() if received_at else None,
        "properties": props,
    }

    fatal = {A_MALFORMED, A_MISSING_TENANT}
    verdict = QUARANTINE if fatal.intersection(anomalies) else (
        ACCEPT_FLAGGED if anomalies else ACCEPT)
    return Verdict(line_no, verdict, obj.get("event_id"), tenant_id, anomalies, detail,
                   canonical, raw if verdict == QUARANTINE else None)


def normalize_batch(lines: List[str]) -> List[Verdict]:
    """Normalize a batch and apply the batch-scoped rules: idempotency on
    event_id, and the burst heuristic that needs more than one event to see."""
    verdicts = [normalize_line(i, ln) for i, ln in enumerate(lines, 1) if ln.strip()]

    # Idempotency. The SDK retries on timeout, so at-least-once delivery is a
    # given; exactly-once *effect* is bought here, keyed on the SDK-generated
    # event_id, which is the only field we can trust to mean "the same event".
    seen: Dict[Tuple[Optional[str], str], int] = {}
    for v in verdicts:
        if v.verdict == QUARANTINE or not v.event_id:
            continue
        key = (v.tenant_id, v.event_id)
        if key in seen:
            first = seen[key]
            v.verdict = DUPLICATE
            v.anomalies.append(A_DUPLICATE_ID)
            v.detail["duplicate_of_line"] = first
        else:
            seen[key] = v.line_no

    # Non-human burst: N+ page_views from one visitor inside a 1s window.
    by_visitor: Dict[Tuple[Optional[str], Optional[str]], List[Verdict]] = {}
    for v in verdicts:
        if v.canonical and v.canonical.get("type") == "page_view":
            by_visitor.setdefault((v.tenant_id, v.canonical.get("anonymous_id")), []).append(v)
    for _, group in by_visitor.items():
        stamped = [(g, _parse_ts(g.canonical.get("ts"))) for g in group]
        stamped = [(g, t) for g, t in stamped if t]
        stamped.sort(key=lambda x: x[1])
        for i in range(len(stamped)):
            window = [g for g, t in stamped
                      if 0 <= (t - stamped[i][1]).total_seconds() <= BURST_WINDOW_S]
            if len(window) >= BURST_MIN_EVENTS:
                for g in window:
                    if A_BOT_BURST not in g.anomalies:
                        g.anomalies.append(A_BOT_BURST)
                    g.detail["bot_signal"] = ("{} page_views from one visitor within {}s"
                                              .format(len(window), BURST_WINDOW_S))
                    if g.verdict == ACCEPT:
                        g.verdict = ACCEPT_FLAGGED
                break

    return verdicts
