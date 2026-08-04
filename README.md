# Beat Claude — Engineer 004: Real-Time Analytics Pipeline

Operating artifact for the Single Grain Beat Claude challenge
[`engineer-004`](https://github.com/ericosiu/beat-claude/tree/main/challenges/engineer-004).
**Brief version: 2026-07.** Written answer: [`ANSWER.md`](ANSWER.md) (also rendered as
`Michael-Barber-BeatClaude-Engineer-004.pdf`). Diagrams: [`ARCHITECTURE.md`](ARCHITECTURE.md).

Fixture worked: `fixtures/event_sample.jsonl`
sha256 `1aeb24b415009e89fcf8acb5a178410faf216dc17b16920d9849ecc8bbb24235`

## Run it

Stock Python 3.9+. Standard library only. No network, no credentials, no install step.

```bash
shasum -a 256 fixtures/event_sample.jsonl   # confirm you have the same fixture round
python3 detect_anomalies.py                 # per-line verdicts + cross-event findings
python3 -m unittest discover -s tests -v    # 20 tests
python3 bench_ingest.py                     # throughput + latency, on your hardware
python3 cost_model.py                       # line-item AWS cost model
python3 cost_model.py --spike-days 2        # same month with two 10x days
node build_pdf.mjs                          # re-render the PDF (needs Chrome)
```

`out/` holds the committed output of each command as run on Darwin arm64 / Python 3.9.6, so a
reviewer can diff their run against mine.

## What each file is

| File | What it is |
|---|---|
| `pipeline/normalize.py` | The ingest-edge contract — the riskiest component, so it exists as code. Verdict machine (`accept` / `accept_flagged` / `duplicate` / `quarantine`), legacy-field translation, PII detection, idempotency, clock/bot/aggregate classification. Nothing is ever dropped. |
| `detect_anomalies.py` | Runs the contract over the fixture. Per-line verdicts plus cross-event findings: identity stitching, GDPR deletion blast radius, client-aggregate contradiction, per-tenant bot share. Exits 1 if anything quarantines. |
| `tests/test_normalize.py` | 20 tests. One per seeded anomaly class, plus the non-loss / idempotency / deletion-cascade invariants the design's claims rest on. Includes a checksum test that fails loudly if the fixture rotates. |
| `bench_ingest.py` | Single-core throughput and per-event latency for the transform, so the sizing arithmetic has a measured floor rather than an asserted one. |
| `cost_model.py` | Line-item AWS cost model. Every volume input and unit price is labelled at the point of use; change one constant and the whole model re-derives. |
| `build_pdf.mjs` | Renders `ANSWER.md` + `DIAGRAMS.txt` to the submission PDF via headless Chrome. |

## Headline findings

- 50M events/day is **579 events/sec**. The normalize-and-classify step benchmarks at
  **112,280 events/sec/core** in CPython, so the entire 10x spike is ~0.05 of one core. This is not
  a scale problem — it is a missing ingest contract.
- Modelled infrastructure cost is **$4,952/month against a $50K ceiling**. Budget is not the
  binding constraint; the two dedicated engineers are.
- The only unparseable line in the fixture (`evt-0020`) is a `#signup` click. Loss is not random —
  it correlates with the highest-value payloads.
- The seeded GDPR request (`evt-0017`) under-deletes: the subject's other event (`evt-0006`) carries
  `user_id: null` and is reachable only through `anon-77a`.

Full reasoning, trade-offs, evidence log, and AI usage disclosure are in [`ANSWER.md`](ANSWER.md).
