# 01 — Five-Component Architecture

## High-level diagram

```
                  ┌────────────────────────────────────────────────────┐
                  │                     gRPC clients                   │
                  │  (mobile, web, internal services)                  │
                  └────────────────────┬───────────────────────────────┘
                                       │  ScoredPostsRequest / ForYouFeedQuery
                                       ▼
              ╔════════════════════════════════════════════════════════╗
              ║                     home-mixer (Rust)                  ║
              ║                                                        ║
              ║  ┌──────────────────────┐   ┌──────────────────────┐   ║
              ║  │ ScoredPostsService   │   │ ForYouFeedService    │   ║
              ║  │ → PhoenixCandidate   │   │ → ForYouCandidate    │   ║
              ║  │   Pipeline           │   │   Pipeline           │   ║
              ║  └────────┬─────────────┘   └────────┬─────────────┘   ║
              ║           │                          │ wraps           ║
              ║           │                          │ ScoredPosts via ║
              ║           │                          │ ScoredPostsSrc  ║
              ║           │                          │ as 1st source   ║
              ╚═══════════╪══════════════════════════╪═════════════════╝
                          │            ┌─────────────┘
                          ▼            ▼
    ┌────────────────┐  ┌────────┐  ┌───────────────┐  ┌──────────────┐
    │ thunder (Rust) │  │ phoenix│  │ Manhattan,    │  │ Other prod   │
    │ in-network     │  │ JAX    │  │ Strato,       │  │ services:    │
    │ post cache     │  │ port   │  │ Redis (ATLA   │  │ Gizmoduck,   │
    │ (5 DashMaps)   │  │ of     │  │ + PDXA),      │  │ VF, Ads,     │
    │ Kafka-fed      │  │ Grok-1 │  │ Kafka topics  │  │ TweetMixer,  │
    │                │  │        │  │               │  │ TES, etc.    │
    └────────┬───────┘  └────┬───┘  └──────┬────────┘  └──────────────┘
             ▲               ▲              ▲
             │ Kafka         │ HTTP/gRPC    │ writes
             │ ingest        │ retrieval    │
             │               │ + ranking    │
             │               │              │
    ┌────────┴───────────────┴──────────────┴───────────────────────┐
    │                     grox (Python DAG executor)                │
    │  Plans → Tasks → Engine → Dispatcher                          │
    │  9 ML pipelines: embeddings, safety, banger detection,        │
    │  reply ranking, spam detection                                │
    └───────────────────────────────────────────────────────────────┘
```

## Component-by-component summary

### 1. `candidate-pipeline` (Rust library)

A generic, trait-based async pipeline framework. Defines the abstract execution model used by home-mixer.

**Provides:**
- 7 component traits: `Source`, `Hydrator`, `QueryHydrator`, `Filter`, `Scorer`, `Selector`, `SideEffect`
- A `CachedHydrator` trait that auto-implements `Hydrator` with cache-aside semantics
- A `CandidatePipeline` super-trait with default `execute()` driving every stage
- The `PipelineQuery` trait (`params()`, `decider()`) — the integration point for feature switches

**Does NOT:** know anything about posts, users, Phoenix, Thunder, or Redis. It is pure plumbing.

See **[components/candidate-pipeline.md](components/candidate-pipeline.md)** for full details.

### 2. `home-mixer` (Rust gRPC service)

The orchestrator. Hosts both gRPC services. Implements two concrete `CandidatePipeline`s:

- **`PhoenixCandidatePipeline`** — the ranker. 49 components wired (15 QueryHydrators, 0 DependentQueryHydrators, 6 Sources, 10 Hydrators, 14 Filters, 3 Scorers, 1 Selector, 6 PostSelectionHydrators, 3 PostSelectionFilters, 6 SideEffects).
- **`ForYouCandidatePipeline`** — the blender. Wraps ScoredPosts via a `ScoredPostsSource`, then blends in ads, WhoToFollow, prompts, and push-to-home via `BlenderSelector`.

See **[components/home-mixer.md](components/home-mixer.md)** for the full inventory.

### 3. `thunder` (Rust gRPC service + Kafka consumer)

In-network post cache. Listens on the `IN_NETWORK_EVENTS` Kafka topic for `TweetCreateEvent` / `TweetDeleteEvent` messages and maintains 5 in-memory DashMaps:

1. `posts` (post_id → `LightPost`) — primary store
2. `original_posts_by_user` (author_id → `VecDeque<TinyPost>`) — originals timeline
3. `secondary_posts_by_user` (author_id → `VecDeque<TinyPost>`) — replies + retweets
4. `video_posts_by_user` (author_id → `VecDeque<TinyPost>`) — video-eligible posts (incl. retweets-of-video inheritance)
5. `deleted_posts` (post_id → bool) — tombstones

Exposes a single gRPC: `GetInNetworkPosts(user_id, following_user_ids, exclude_tweet_ids, is_video_request, max_results)`. Auto-trim every 2 minutes; default retention 2 days.

See **[components/thunder.md](components/thunder.md)** for storage layout, ingest pipeline, retention.

### 4. `phoenix` (Python JAX/Haiku model)

The ML brain — a Grok-1-derived transformer adapted for recommendation:

- **Retrieval:** two-tower (Phoenix transformer user tower vs. SiLU-activated 2-layer MLP candidate tower) → dot-product top-K
- **Ranking:** Phoenix transformer with **candidate-isolation attention mask** — candidates cannot attend to each other (would entangle independent decisions)
- **Action heads:** 19 discrete + 8 continuous (only `dwell_time` at index 1 actually consumed downstream)

Block structure: RMSNorm (init 0) + RoPE (base 1e4) + **GEGLU** FFN + sequential double-normalized residual + `30*tanh(x/30)` attention logit clamp.

See **[components/phoenix.md](components/phoenix.md)** for transformer internals.

### 5. `grox` (Python DAG executor)

Off-path pipeline runner for ML tasks. Hosts 9 `Plan`s (PlanInitialBanger, PlanPostSafety, PlanSpamComment, PlanPostEmbeddingWithSummary[ForReply], PlanPostEmbeddingV5[ForReply], PlanReplyRanking, PlanSafetyPtos).

Each Plan is a DAG of `Task`s, run via `asyncio.gather` with skip propagation (any dep skipped → dependent skipped). Output sinks: Kafka (`GROX_CONTENT_ANALYSIS`) and Manhattan / Strato (UPA, multimodal embedding sinks).

Three retry layers: task (@retry stop=2, wait=1s), sink (@retry stop=3, wait_chain), dispatcher init (incrementing backoff capped at 9s).

See **[components/grox.md](components/grox.md)** for plan inventory and execution model.

---

## Dependencies between components

```
candidate-pipeline ──[library dep]──> home-mixer
                                         │
                                         ├──[gRPC]──> thunder
                                         ├──[gRPC/HTTP]──> phoenix (inference cluster)
                                         ├──[gRPC]──> Gizmoduck, TES, TweetMixer, VF, Ads, Strato
                                         └──[Redis]──> ATLA + PDXA caches

phoenix ──[reads]──> trained model artifacts (LFS pointer in repo)

grox ──[publishes]──> Kafka GROX_CONTENT_ANALYSIS
     ──[writes]──> Manhattan via Strato
     ──[NOT on request path]
```

`grox` is offline / training-time; it is not in the synchronous request lifecycle.

---

Next: **[02-request-lifecycle.md](02-request-lifecycle.md)** walks through what happens on a real request.
