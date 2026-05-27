# 00 — Executive Summary & Key Facts

## What this system does

For each home-timeline request, the system:
1. Hydrates the request with user-side context (followed users, blocked users, demographics, cached posts).
2. Fetches candidates from multiple sources (Thunder for in-network posts, Phoenix for ML-retrieved out-of-network, TweetMixer, etc.).
3. Hydrates candidate-side metadata.
4. Filters out ineligible candidates (already-seen, blocked authors, muted keywords, etc.).
5. Scores them via the Phoenix transformer + a feature-weighted RankingScorer (+ optional VMRanker for DPP diversity).
6. Selects top-K (TopKScoreSelector).
7. Runs post-selection hydration + filtering (visibility filtering, ads brand safety, conversation dedup).
8. For `ForYouFeedService`, blends in ads / WhoToFollow / prompts / push-to-home and pins them at the right positions.
9. Fires side effects (Kafka publishing, Redis cache writes to ATLA+PDXA, served-history updates).

## Key facts at a glance

| Dimension | Value |
|-----------|-------|
| Languages | Rust (3 services), Python+JAX/Haiku (1 model), Python (1 DAG runner) |
| Top-level RPC entrypoints | `ScoredPostsService.get_scored_posts`, `ScoredPostsService.get_debug_scored_posts`, `ForYouFeedService.get_for_you_feed`, `ForYouFeedService.get_for_you_feed_urt` |
| ML core | Grok-1-derived transformer; **bfloat16** default; sequential attention+FFN residual with **double normalization** |
| Action space | **19 discrete actions** + **8 continuous predictions** (only `dwell_time` actually consumed downstream) |
| Retrieval | Two-tower: Phoenix transformer user tower vs. 2-layer **SiLU** MLP candidate tower |
| FFN type | **GEGLU** (`jax.nn.gelu`) in the transformer (NOT SwiGLU; candidate-tower MLP uses SiLU separately) |
| Cache | thunder = **5** `DashMap`s; default **2-day** retention; trim every 2 minutes |
| Pipeline framework | **7 component traits** + 2 additional **PipelineStage slots** (post-selection hydrators/filters reuse Hydrator/Filter) |
| Datacenter awareness | ATLA + PDXA Redis replicas; `--datacenter` flag participates in feature-switch recipient matching |
| Cached-posts fast path | When ≥500 cached posts exist, **Thunder + Phoenix + TweetMixer + PhoenixScorer all auto-disable** via `enable()` hooks |
| Global pipeline timeout | **NONE** — only per-component local timeouts |
| Circuit breaker | **NONE** |

## Glossary

| Term | Meaning |
|------|---------|
| **Candidate** | A `PostCandidate` (or `FeedItem` for ForYou) — a piece of content under consideration for the feed |
| **Hydrator** | Async stage that enriches candidates with metadata (length-preserving) |
| **QueryHydrator** | Async stage that enriches the *request* with user-side context |
| **DependentQueryHydrator** | Same `QueryHydrator` trait, but executed in a second phase after the first phase completes — lets it read fields produced by phase 1 |
| **Filter** | Sync stage that explicitly drops candidates (partitions kept/removed) — the ONLY stage allowed to drop |
| **Scorer** | Async stage that assigns scores to candidates (length-preserving) |
| **Selector** | Sync stage that sorts + truncates (default TopKScoreSelector) |
| **SideEffect** | Async fire-and-forget action after selection (Kafka publish, Redis cache write, etc.) — errors discarded |
| **ScoredPostsService** | gRPC service that runs PhoenixCandidatePipeline → returns ranked posts |
| **ForYouFeedService** | gRPC service that runs ForYouCandidatePipeline → wraps ScoredPosts + blends ads/WTF/prompts |
| **Phoenix** | The JAX/Haiku transformer model (Grok-1-derived) — does retrieval & ranking |
| **Grox** | Python DAG executor for ML pipelines (training-time / offline) — separate from the request path |
| **Thunder** | Rust in-memory in-network post cache, fed by Kafka |
| **VF** | Visibility Filtering — safety-and-policy filter applied post-selection |
| **OON** | Out Of Network — posts whose authors the user does NOT follow |
| **DPP** | Determinantal Point Process — diversity-oriented selection used by VMRanker |
| **MOE** | Mixture of Experts — variant Phoenix retrieval source served by a separate Phoenix cluster (`PhoenixRetrievalMOEInferenceClusterId`). Gated by the `EnablePhoenixMOESource` feature switch; runs in parallel with the standard `PhoenixSource` when enabled. |
| **VF** | Visibility Filtering — XAI's safety-and-policy filter applied post-selection. Hydrates a verdict per candidate (500 ms timeout), then `VFFilter` / `AncillaryVFFilter` drop candidates whose verdict fails. |
| **URT** | Unified Response Timeline — the wire format used by mobile clients; ForYouFeedService's `get_for_you_feed_urt` serializes results as URT thrift binary with cursor-based pagination. |
| **TES** | Tweet Entity Service — internal microservice that returns canonical tweet data; called by `CoreDataCandidateHydrator` and `VideoDurationCandidateHydrator`. |

---

See **[01-architecture.md](01-architecture.md)** for the five-component diagram next.
