# 08 — Cross-Cutting Patterns

## 1. Cached-posts fast path

The single most impactful optimization in home-mixer. When a request hits the Redis post candidate cache, an entire sub-graph of components silently disables via the `enable()` hook — **no central routing logic**.

```
   1. Request enters PhoenixCandidatePipeline
       │
       ▼
   2. CachedPostsQueryHydrator (cached_posts_query_hydrator.rs)
       • Feature-switch gated (EnableCachedPosts)
       • 300 ms Redis GET
       • zstd-decompresses JSON payload
       • Sets query.has_cached_posts = true
         ONLY IF >= 500 posts present                      ◀── threshold
       │
       ▼
       ┌──────────────────────────────────────────────────────────┐
       │  if query.has_cached_posts:                              │
       │                                                          │
       │     ┌── CachedPostsSource              ─── ENABLED  ✓    │
       │     │   (cached_posts_source.rs:10)                      │
       │     │                                                    │
       │     ├── ThunderSource                  ─── DISABLED ✗    │
       │     │   (thunder_source.rs:19)                           │
       │     │                                                    │
       │     ├── PhoenixSource                  ─── DISABLED ✗    │
       │     │   (phoenix_source.rs:63-67)                        │
       │     │   PhoenixTopicsSource            ─── DISABLED ✗    │
       │     │   (phoenix_topics_source.rs:29)                    │
       │     │   PhoenixMOESource               ─── DISABLED ✗    │
       │     │   (phoenix_moe_source.rs:24)                       │
       │     │                                                    │
       │     ├── TweetMixerSource               ─── DISABLED ✗    │
       │     │   (tweet_mixer_source.rs:22)                       │
       │     │                                                    │
       │     └── PhoenixScorer                  ─── DISABLED ✗    │
       │         (phoenix_scorer.rs:63-65)                        │
       │                                                          │
       │  ► All decisions made via component's enable() hook,     │
       │    not via central routing logic.                        │
       │                                                          │
       │  ► RankingScorer intentionally STILL RUNS (no cache      │
       │    check, ranking_scorer.rs:244) — feature-weighted      │
       │    re-scoring is applied to the cached set.              │
       └──────────────────────────────────────────────────────────┘
       │
       ▼
   3. Cache populated by RedisPostCandidateCacheSideEffect (prod-only)
       • TTL: 180 seconds
       • Codec: zstd level 6
       • Source: selected ∪ non_selected filtered to weighted_score > 0,
                 sorted desc, truncated to MaxPostsToCache
```

This pattern lets home-mixer skip **both** retrieval AND Phoenix ranking for users whose recent results are still warm, while keeping the pipeline structure identical (just with disabled stages).

## 2. Observability

### Distributed tracing

```
   ┌─────────────────────────────────────────────────────────────┐
   │  B3 trace context extraction at gRPC ingress                │
   │    (server.rs:212, 229)                                     │
   │                                                             │
   │  Root span: "request"                                       │
   │    Fields: {endpoint, trace, user, b3}                      │
   │                                                             │
   │  Pipeline child spans (all instrumented in                  │
   │  candidate-pipeline crate):                                 │
   │    query_hydrators, dependent_query_hydrators,              │
   │    sources, hydrators, filters, post_selection_filters,     │
   │    scorers                                                  │
   │                                                             │
   │  Per-component spans:                                       │
   │    source, hydrator, scorer, query_hydrator                 │
   │  with fields: {name, counts, rates}                         │
   │                                                             │
   │  Optional OTEL endpoint via --otel_endpoint                 │
   └─────────────────────────────────────────────────────────────┘
```

### Metric receivers

Latency / size buckets (selected examples):

| Component | Bucket |
|-----------|--------|
| Source | `size = Bucket500To1000` |
| Hydrator | `latency = Bucket50To500`, `size = Bucket500To2500` |
| Filter | `latency = Bucket0To50` |
| Selector | `latency = Bucket0To50`, `size = Bucket0To50` |
| Pipeline `execute` | `latency = Bucket500To2500` |

Counters/scopes:

| Metric | Scopes |
|--------|--------|
| `{filter_name}.run` | `kept`, `removed` |
| `{cached_hydrator}.cache` | `cache_hit`, `cache_miss` |
| `{pipeline_name}.execute` | `result_size`, `result_empty` |

## 3. Backpressure

Backpressure is **not centralized**. There is no global semaphore on home-mixer's fanout. It lives at boundary services:

| Boundary | Mechanism | File:Line |
|----------|-----------|-----------|
| Thunder gRPC | `try_acquire()` → `RESOURCE_EXHAUSTED` on overload | `thunder_service.rs:160-171` |
| Thunder Kafka ingest | Bound-3 semaphore (reserves CPU for serving during init burst) | `tweet_events_listener_v2.rs:56, 214-219` |
| Grox dispatcher | `max_in_flight` gauge in `_fill_loop` | `dispatcher.py:251` |

## 4. Fault isolation

The pipeline is built to *not fail the request* on single-component failure.

```
   ┌────────────────────────────────────────────────────────────┐
   │  A single Source failure does NOT kill the request.        │
   │                                                            │
   │  candidate_pipeline.rs:262-266                             │
   │      let results = join_all(source_futures).await;         │
   │      let merged = results.into_iter().flatten();           │
   │                                       ▲                    │
   │                          flatten() drops Err silently      │
   │                                                            │
   │  Hydrator/Scorer failures: per-entry, length-preserving.   │
   │  Filter failures: impossible by signature (sync, infallible│
   │    internal API; FilterResult is always returned).         │
   │  Selector failures: impossible by signature.               │
   │  SideEffect failures: discarded (fire-and-forget).         │
   └────────────────────────────────────────────────────────────┘
```

**Only retry-like behavior on the request path**: PhoenixScorer falls back from egress sidecar → direct Phoenix client (`phoenix_scorer.rs:86-98`).

## 5. Feature switching

Every component's `enable()` hook reads from `PipelineQuery::params()` and `decider()` accessors. The feature-switch system matches a recipient with these fields:

```
   user_id      country     language     client_app_id
   datacenter   account_age_days         phone_number      user_roles
```

(`server.rs:138-175`)

This single hook is the lever for:
- A/B tests
- Country / language gating
- Datacenter-specific experiments
- New-user behavior
- Role-based dark-launch

## 6. Datacenter awareness

```
   ATLA ◀───┐                       ┌───▶ PDXA
            │  Phoenix Request      │
            │  Cache writes to      │
            │  BOTH replicas        │
            │  in parallel          │
            │  (phoenix_request_    │
            │   cache_side_effect.rs│
            │   :92-111)            │
            │                       │
            │                       │
   home-mixer ──── decides via ─────┘
   --datacenter flag, recipient-matched feature switches,
   shard-aware Strato / TES / Gizmoduck / TweetMixer / VF clients.
```

`--shard_coordinate (ordinal, total_size)` + `--datacenter` are constructor args for every shard-aware client. Datacenter participates in feature-switch recipient matching as a custom string (`server.rs:150`).

## 7. The `enable()` hook is the universal lever

Re-emphasized because of how pervasive this is. Every trait in `candidate-pipeline` defines:

```rust
fn enable(&self, query: &Q) -> bool { true }
```

Default `true`, override as needed. The framework calls it once per stage per component, adds disabled component names to the span's `disabled` field for observability, and skips the disabled components.

This single mechanism implements:
- The cached-posts fast path (Sources, PhoenixScorer disable themselves)
- A/B tests (RankingScorer reads weights from `query.params()` and may use different weights or be disabled altogether for some users)
- Datacenter routing
- New-user behavior (separate OON weights, separate retrieval source)
- Topic-request handling (different sources for topic vs. bulk-topic vs. general)
