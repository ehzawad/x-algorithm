# X "For You" Recommendation Algorithm — Technical Documentation

> Source repository: `/Users/ehz/x-algorithm` (May 2026 open-source release)
> Compiled: 2026-05-28
> Method: three audit layers — initial draft by 4 parallel Claude exploration agents, then a 4-agent Claude review pass, then a 4-role Codex council audit. Material corrections from all three layers have been applied. See `REPORT.md` §11 for the full provenance.
> Every claim is grounded in a `file:line` citation; reconciled against the actual source.

---

## What this repository is

This is the production system that decides — for each user request — which posts to surface in the X (formerly Twitter) home timeline's "For You" feed. It is composed of **five components** linked by gRPC and Kafka:

| # | Component | Language | Role |
|---|-----------|----------|------|
| 1 | `candidate-pipeline` | Rust | Generic trait framework for stage-by-stage candidate processing |
| 2 | `home-mixer` | Rust | gRPC orchestrator hosting `ScoredPostsService` and `ForYouFeedService` |
| 3 | `thunder` | Rust | Kafka-fed in-network post cache (5 DashMaps) |
| 4 | `phoenix` | Python (JAX/Haiku) | Grok-1-derived transformer for retrieval + ranking |
| 5 | `grox` | Python | DAG executor for offline/training ML pipelines |

---

## Reading order

Start here, then follow either the "fast tour" or the "deep dive" route depending on your need.

### Fast tour (≈15 min)
1. **[00-overview.md](00-overview.md)** — Executive summary, key facts at a glance, glossary
2. **[01-architecture.md](01-architecture.md)** — The five-component diagram and how data flows
3. **[02-request-lifecycle.md](02-request-lifecycle.md)** — What happens when a client calls `get_scored_posts`
4. **[REPORT.md](REPORT.md)** — Single-file comprehensive technical report (all sections)

### Deep dive (per component)
- **[components/candidate-pipeline.md](components/candidate-pipeline.md)** — Trait framework, stage semantics, observability
- **[components/home-mixer.md](components/home-mixer.md)** — gRPC services, 49 wired components, fast-path logic
- **[components/thunder.md](components/thunder.md)** — 5-DashMap layout, Kafka ingestion, retention
- **[components/phoenix.md](components/phoenix.md)** — Grok-1 block, candidate-isolation attention, two towers, action heads
- **[components/grox.md](components/grox.md)** — Plan/Task/Engine/Dispatcher, 9 plans, fault tolerance

### Cross-cutting
- **[08-cross-cutting-patterns.md](08-cross-cutting-patterns.md)** — Cached-posts fast path, observability, backpressure, fault isolation, feature switching, datacenter awareness
- **[09-checkout-completeness.md](09-checkout-completeness.md)** — What's in this open-source release vs. what's external

---

## Provenance

This documentation was generated through three audit layers:

**Layer 1 — Initial draft: four parallel Claude exploration agents** covering candidate-pipeline (traits), home-mixer (64 wired components in PhoenixCandidatePipeline + 16 in ForYou), thunder (5-DashMap storage + Kafka), and phoenix + grox (ML model + DAG executor).

**Layer 2 — Audit: four Claude review agents** — a citation line-number spot-checker (≥30 samples), a diagram & table verifier (10 diagrams), a completeness gap-finder, and a holistic prose-polish reviewer.

**Layer 3 — Audit: a four-role Codex council** — rust-pipeline-auditor, thunder-storage-auditor, phoenix-ml-auditor, and citation-spot-checker, each in its own `codex exec` session.

Material corrections from all three layers have been merged into the docs. The `Verification provenance` appendix of **[REPORT.md](REPORT.md)** lists every correction with its source-of-truth citation.

---

## Conventions used in this documentation

- All factual claims include a `file:line` or `file:lineA-lineB` citation. Open them with `code -g <path>:<line>` to navigate directly.
- "Verified" means a claim was checked against the source after first-pass synthesis. Plain claims still cite the source but may not have been double-audited.
- Diagrams are rendered in monospaced ASCII so they render identically in every Markdown viewer.
- When this repository is a "reference release" with external dependencies missing, the missing modules are explicitly listed in **[09-checkout-completeness.md](09-checkout-completeness.md)**.

---

*End of index.*
