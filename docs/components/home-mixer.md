# Component Deep-Dive — `home-mixer` (Rust)

The gRPC orchestrator. Hosts two services that drive two concrete `CandidatePipeline`s built on the `candidate-pipeline` framework.

## CLI and service setup

```rust
// main.rs:13-44
#[arg(long, default_value = "50051")]   grpc_port: u16,
#[arg(long, default_value = "9090")]    metrics_port: u16,
#[arg(long, default_value = "-1")]      shard_coordinate: i16,    // ordinal
#[arg(long, default_value = "500")]     shard_total_size: u16,
#[arg(long, default_value = "atla")]    datacenter: String,
#[arg(long, default_value = "")]        otel_endpoint: String,
```

```rust
// main.rs:48-61
XServiceBuilder::new(...)
    .with_featureswitches(params::FS_PATH, true)
    .with_decider(params::decider_path(), None)
    .with_tls(TlsMode::server_mtls_from_env()?)
    .with_max_connection_age(Duration::from_secs(300))
    .with_reflection(pb::FILE_DESCRIPTOR_SET)
    .with_layer(dark_traffic_setup::resolve_layer())
    .with_layer(RejectDarkTrafficLayer::from_env())
```

Both gRPC services are registered with **Gzip + Zstd** compression on ingress and egress (`server.rs:397-419`).

## Two services, two pipelines

| Service | Pipeline | Returns | Purpose |
|---------|----------|---------|---------|
| `ScoredPostsService.get_scored_posts` (`server.rs:206`) | `PhoenixCandidatePipeline` | Ranked `PostCandidate`s | The "ranker" — does retrieval, hydration, filtering, scoring, selection |
| `ScoredPostsService.get_debug_scored_posts` (`server.rs:237-267`) | Same | JSON debug bundle | Forces trace sampling, accepts `fs_overrides`, returns full pipeline state (query + retrieved + filtered + selected) for offline analysis |
| `ForYouFeedService.get_for_you_feed` (`for_you_server.rs:28`, `server.rs:273`) | `ForYouCandidatePipeline` | Mixed `FeedItem`s | The "blender" — wraps ScoredPosts + interleaves ads/WTF/prompts/push-to-home |
| `ForYouFeedService.get_for_you_feed_urt` (`for_you_server.rs:43`, `server.rs:301-346`) | Same | URT thrift binary | Same pipeline, but decodes a pagination cursor and emits the URT timeline format consumed by mobile clients |

## PhoenixCandidatePipeline — 64 wired components

All wired in `candidate_pipeline/phoenix_candidate_pipeline.rs:185-338`.
Counts include the single Selector and the (default-empty) DependentQueryHydrator slot.

### QueryHydrators (15)

| # | Name | Notes |
|---|------|-------|
| 1 | `ScoringSequenceQueryHydrator` | user action sequence for scoring |
| 2 | `RetrievalSequenceQueryHydrator` | user action sequence for retrieval |
| 3 | `BlockedUserIdsQueryHydrator` | social graph |
| 4 | `MutedUserIdsQueryHydrator` | social graph |
| 5 | `FollowedUserIdsQueryHydrator` | following list |
| 6 | `SubscribedUserIdsQueryHydrator` | subscriptions |
| 7 | `CachedPostsQueryHydrator` | 300 ms Redis GET; sets `has_cached_posts=true` only if ≥500 posts |
| 8 | `MutualFollowQueryHydrator` | mutual follow relationships |
| 9 | `UserDemographicsQueryHydrator` | demographics |
| 10 | `FollowedGrokTopicsQueryHydrator` | 300 ms timeout |
| 11 | `FollowedStarterPacksQueryHydrator` | 300 ms timeout |
| 12 | `InferredGrokTopicsQueryHydrator` | Strato call |
| 13 | `ImpressionBloomFilterQueryHydrator` | impression bloom filter |
| 14 | `IpQueryHydrator` | geo-IP |
| 15 | `UserInferredGenderQueryHydrator` | inferred gender |

`ImpressedPostsQueryHydrator` is defined but **not wired** (line 234-236).

### DependentQueryHydrators (0)

PhoenixCandidatePipeline does **not** override `dependent_query_hydrators()`, so this stage is empty. The framework default is `&[]`.

### Sources (6)

| # | Source | `enable()` condition |
|---|--------|----------------------|
| 1 | `ThunderSource` | `!has_cached_posts` |
| 2 | `TweetMixerSource` | `!in_network_only && !has_cached_posts` |
| 3 | `PhoenixSource` | `(!is_topic_request() \|\| is_bulk_topic_request()) && (!EnableNewUserTopicRetrieval \|\| !has_new_user_topic_ids()) && !in_network_only && !has_cached_posts` (phoenix_source.rs:63-67) |
| 4 | `PhoenixTopicsSource` | `((is_topic_request() && !is_bulk_topic_request()) \|\| (EnableNewUserTopicRetrieval && has_new_user_topic_ids())) && !in_network_only && !has_cached_posts` (phoenix_topics_source.rs:27-29) |
| 5 | `PhoenixMOESource` | `EnablePhoenixMOESource && (!is_topic_request() \|\| is_bulk_topic_request()) && !in_network_only && !has_cached_posts` (phoenix_moe_source.rs:24) |
| 6 | `CachedPostsSource` | `has_cached_posts` (only on cache hit) |

### Hydrators (10)

| # | Hydrator |
|---|----------|
| 1 | `InNetworkCandidateHydrator` |
| 2 | `CoreDataCandidateHydrator` (TES) |
| 3 | `QuoteHydrator` (200 ms video duration) |
| 4 | `VideoDurationCandidateHydrator` |
| 5 | `HasMediaHydrator` |
| 6 | `SubscriptionHydrator` |
| 7 | `GizmoduckCandidateHydrator` |
| 8 | `BlockedByHydrator` |
| 9 | `FilteredTopicsHydrator` |
| 10 | `LanguageCodeHydrator` |

### Filters (14)

`DropDuplicatesFilter`, `CoreDataHydrationFilter`, `AgeFilter`, `SelfTweetFilter`, `RetweetDeduplicationFilter`, `IneligibleSubscriptionFilter`, `PreviouslySeenPostsFilter`, `PreviouslySeenPostsBackupFilter`, `PreviouslyServedPostsFilter`, `MutedKeywordFilter`, `AuthorSocialgraphFilter`, `VideoFilter`, `TopicIdsFilter`, `NewUserTopicIdsFilter`.

### Scorers (3)

| # | Scorer | `enable()` | What it does |
|---|--------|-----------|---------------|
| 1 | `PhoenixScorer` | `!has_cached_posts` (`phoenix_scorer.rs:63-65`) | Calls Phoenix inference; egress sidecar → direct client fallback on error (`phoenix_scorer.rs:86-98`). Produces `phoenix_scores` (19 discrete action probs + 8 continuous). |
| 2 | `RankingScorer` | always (`ranking_scorer.rs:244-245`) | NOT a simple linear combiner — see below |
| 3 | `VMRanker` | `EnableVMRanker` feature switch (`vm_ranker.rs:18-20`) | DPP-based diversity ranking; tunable theta / max-selected-rank |

**`RankingScorer` is NOT a linear combination of action probs** (`ranking_scorer.rs`):
- **Feature-switch-driven action weights** — 22 configurable weights, loaded per-request from `query.params`. Positive: favorite, reply, retweet, photo_expand, click, profile_click, vqv, share variants, dwell, quote, quoted_click, quoted_vqv, follow_author. Negative: not_interested, block_author, mute_author, report, not_dwelled. (`:42-114`)
- **Continuous dwell terms** — `ContDwellTimeWeight` and `ContClickDwellTimeWeight` (`:56-58, 163-164`) add weighted continuous predictions on top of the discrete action-probability sum.
- **Author-diversity penalty** — exponential decay per author position within the candidate list (`:190-217`). Posts from authors already represented earlier are progressively down-weighted.
- **OON (out-of-network) weighting** — three branches (`:220-239`):
  - Topic requests: `TopicOonWeightFactor`
  - New users (age < threshold, following count ≥ minimum): `NEW_USER_OON_WEIGHT_FACTOR`
  - All others: `OonWeightFactor`
- **Video eligibility filter** — `MinVideoDurationMs` (`:65, 132-144`) drops a candidate's score below the video-eligibility threshold when its duration is too short.

**`VMRanker` (DPP diversity ranker)** (`vm_ranker.rs:12-132`):
- Gated by `EnableVMRanker`. Calls a separate value-model cluster (`VMRankerClusterId`) with the Phoenix scores as input.
- `VMRankerDppTheta` (`:111`) controls the DPP kernel diversity-vs-relevance tradeoff.
- `VMRankerDppMaxSelectedRank` (`:112`) caps how far down the ranked list DPP re-ordering is applied.
- `VMRankerValueModelId` (`:127`) picks which value model to use.
- Output: candidate.score is replaced with the VMRanker score, so this stage's output feeds straight into `TopKScoreSelector`.

### Selector

`TopKScoreSelector` (`top_k_score_selector.rs:6-15`):

```rust
fn score(&self, candidate: &PostCandidate) -> f64 {
    candidate.score.unwrap_or(f64::NEG_INFINITY)
}
fn size(&self) -> Option<usize> {
    Some(params::TOP_K_CANDIDATES_TO_SELECT)
}
```

Sort descending by `score`, truncate to `TOP_K_CANDIDATES_TO_SELECT`.

### PostSelectionHydrators (6)

`VFCandidateHydrator`, `AdsBrandSafetyHydrator`, `AdsBrandSafetyVfHydrator` (500 ms), `TweetTypeMetricsHydrator`, `FollowingRepliedUsersHydrator`, `MutualFollowJaccardHydrator`.

### PostSelectionFilters (3)

`VFFilter`, `AncillaryVFFilter`, `DedupConversationFilter`.

> `DedupConversationFilter` deduplicates candidates from the same conversation thread by keeping only the post with the **minimum tweet_id in the `ancestors` field** (`dedup_conversation_filter.rs:41-48`) — i.e. the earliest known ancestor of the thread, not the conversation root. Two retweets/replies sharing an ancestor are collapsed to one.

### SideEffects (6)

`PhoenixExperimentsSideEffect`, `RerankingKafkaSideEffect`, `RedisPostCandidateCacheSideEffect` (TTL 180 s, zstd level 6, prod-only), `ScoredStatsSideEffect`, `MutualFollowStatsSideEffect`, `PhoenixRequestCacheSideEffect` (writes ATLA **and** PDXA in parallel).

## ForYouCandidatePipeline — the blender

`for_you_candidate_pipeline.rs:48-278`. The pipeline implements `CandidatePipeline` but returns empty slices for `hydrators()`, `filters()`, `scorers()`, `post_selection_hydrators()`, `post_selection_filters()` (lines 238-278). All the work is in Sources + the Selector.

### QueryHydrators (2)

- `ServedHistoryQueryHydrator`
- `PastRequestTimestampsQueryHydrator`

### Sources (5)

| # | Source | Notes |
|---|--------|-------|
| 1 | `ScoredPostsSource` | Calls `scored_posts_server.run_pipeline(query.clone())` — the entire PhoenixCandidatePipeline runs here |
| 2 | `AdsSource` | AdIndex |
| 3 | `WhoToFollowSource` | WTF recommendations |
| 4 | `PromptsSource` | engagement prompts |
| 5 | `PushToHomeSource` | manually pinned posts |

### Selector — `BlenderSelector`

```
1. Partition by type: posts | ads | wtf_modules | prompts | push_to_home
2. Pick blender:
   - "safe_gap" → SafeGapAdsBlender (ensures min gaps between ads)
   - else      → PartitionOrganicAdsBlender (default)
3. Blend ads into posts
4. Insert prompts at PROMPTS_POSITION
5. Insert WTF at WHO_TO_FOLLOW_POSITION
6. Pin push_to_home at position 0
```

(`blender_selector.rs:10-70`)

### SideEffects (8)

`AdsInjectionLoggingSideEffect`, `PublishSeenIdsToKafkaSideEffect`, `ServedCandidatesKafkaSideEffect`, `ClientEventsKafkaSideEffect`, `ForYouResponseStatsSideEffect`, `UpdatePastRequestTimestampsSideEffect`, `UpdateServedHistorySideEffect`, `TruncateServedHistorySideEffect`.

## URT timeline output (mobile path)

`get_for_you_feed_urt` (`server.rs:301-346`, `for_you_server.rs:43`) runs the same `ForYouCandidatePipeline` as `get_for_you_feed`, but additionally:

1. Decodes a pagination cursor via `cursor_utils::decode_ordered_cursor`.
2. Sets `is_bottom_request` / `is_top_request` / `is_polling` flags on the query based on cursor direction.
3. Serializes the response as URT thrift binary via `xai_urt_thrift::serialize_binary()`.

This is the path mobile clients hit. The non-URT `get_for_you_feed` returns the raw proto with no cursor handling, used by internal services and tests.

## Request entry point

`server.rs:206-234` (`get_scored_posts`):

```rust
// 1. B3 trace extraction
let b3_info = extract_b3_info(request.metadata());

// 2. QueryBuilder.build()
//    - reject viewer_id == 0
//    - force-sample TRACE_USER_IDS
//    - Gizmoduck viewer_data fetch (200 ms timeout, server.rs:177-187)
//    - in_network_only computation
//    - evaluate_feature_switches with recipient:
//        user_id, country, language, client_app_id,
//        custom_string("datacenter"), custom_i64("account_age_days"),
//        custom_bool("has_phone_number"), user_roles
//    - create root span "request" with {endpoint, trace, user, b3}
let ctx = self.query_builder.build(b3_info, request.into_inner(), Default::default(), "scored_posts").await?;

// 3. Run pipeline
let output = self.run_pipeline(query).instrument(root_span).await?;
```

`scored_posts_server.rs:41-77` (`run_pipeline`):

```rust
// Test-user short-circuit
if params::TEST_USER_IDS.contains(&query.user_id) {
    return Ok(PipelineOutput {
        scored_posts: vec![],
        pipeline_result: PipelineResult::empty(),
    });
}

let pipeline_result = self.phoenix_candidate_pipeline.execute(query).await;
// ... convert to proto, log stats ...
```

## Cached-posts fast path

The single most impactful pattern in home-mixer. See **[../08-cross-cutting-patterns.md](../08-cross-cutting-patterns.md)** for the full diagram.

When `CachedPostsQueryHydrator` finds ≥500 fresh posts in Redis (TTL 180 s):
- `query.has_cached_posts = true`
- `CachedPostsSource.enable()` → **on**
- `ThunderSource`, `TweetMixerSource`, `PhoenixSource`, `PhoenixTopicsSource`, `PhoenixMOESource` → all **off** via their `enable()` hooks
- `PhoenixScorer.enable()` → **off** (`phoenix_scorer.rs:63-65`)
- `RankingScorer` still runs (it does not check the cache flag)

This bypass replaces the full retrieval fan-out + Phoenix inference with a single Redis lookup, while keeping all filters and `RankingScorer` so feature-weighted re-scoring still happens on the cached set.

## Cache population

`RedisPostCandidateCacheSideEffect` (`redis_post_candidate_cache_side_effect.rs:13-92`):

```
enable: is_prod() && !query.has_cached_posts
TTL:    180 s
Codec:  zstd level 6
Source: selected ∪ non_selected (filtered to weighted_score > 0, sorted desc, truncated to MaxPostsToCache)
```

## Datacenter / sharding

- `--shard_coordinate (ordinal, total_size)` and `--datacenter` are passed to every shard-aware client (Strato, TES, Gizmoduck, TweetMixer, VF).
- Datacenter participates in feature-switch recipient matching (`server.rs:150` via `custom_string("datacenter", ...)`).
- `PhoenixRequestCacheSideEffect` writes to **both** ATLA and PDXA Redis replicas in parallel (`phoenix_request_cache_side_effect.rs:92-111`).

## Timeouts (request path)

There is **no global pipeline timeout, no circuit breaker.** Local timeouts:

| Location | Timeout |
|----------|---------|
| Gizmoduck viewer data | 200 ms (`server.rs:177-187`) |
| Cached Redis GET | 300 ms (`cached_posts_query_hydrator.rs:12`) |
| Followed Grok topics | 300 ms |
| Followed starter packs | 300 ms |
| Quote video duration | 200 ms |
| VF safety labels | 500 ms (`phoenix_candidate_pipeline.rs:540`) |
| Max gRPC connection age | 300 s (`main.rs:56`) |
| Gizmoduck timeout source | `VIEWER_ROLES_TIMEOUT_MS = 200` at `server.rs:33`, applied at `:178` |

## Summary table

| Component type | PhoenixCandidatePipeline | ForYouCandidatePipeline |
|---|---|---|
| QueryHydrators | 15 | 2 |
| DependentQueryHydrators | 0 | 0 |
| Sources | 6 | 5 |
| Hydrators | 10 | 0 |
| Filters | 14 | 0 |
| Scorers | 3 | 0 |
| Selector | TopKScoreSelector | BlenderSelector |
| PostSelectionHydrators | 6 | 0 |
| PostSelectionFilters | 3 | 0 |
| SideEffects | 6 | 8 |
| **Total wired** | **64** | **16** |

(ForYou counts: 2 query hydrators + 5 sources + 1 selector + 8 side effects = 16. The earlier "17" total was an off-by-one summation error.)
