# 02 — Request Lifecycle

## ScoredPostsRequest (the ranker path)

```
   Client
     │  ScoredPostsRequest (proto)
     ▼
┌────────────────────────────────────────────────────────────────────────┐
│ home-mixer/server.rs:205-234   ScoredPostsService::get_scored_posts    │
│                                                                        │
│   1. B3 trace extraction          (server.rs:212)                      │
│   2. QueryBuilder.build():                                             │
│      - reject missing viewer_id   (server.rs:66-68)                    │
│      - force-sample trace users   (server.rs:69-71)                    │
│      - Gizmoduck viewer_data      (200 ms timeout — server.rs:177-187) │
│      - compute in_network_only    (server.rs:75-76)                    │
│      - evaluate feature switches  (recipient = user, country, lang,    │
│        client_app_id, datacenter, account_age_days,                    │
│        has_phone_number, roles    — server.rs:138-175)                 │
│      - create root span "request" (server.rs:123-129)                  │
│        fields: {endpoint, trace, user, b3}                             │
│   3. self.run_pipeline(query).instrument(root_span)   (server.rs:227)  │
└──────────────────────────┬─────────────────────────────────────────────┘
                           ▼
┌────────────────────────────────────────────────────────────────────────┐
│ scored_posts_server.rs:41-77   ScoredPostsServer::run_pipeline         │
│   • Short-circuits TEST_USER_IDS → empty output   (sps.rs:45-49)       │
│   • Logs request info                            (sps.rs:52)           │
│   • self.phoenix_candidate_pipeline.execute(query).await   ◀───────┐   │
│   • Logs response stats                                            │   │
│   • Converts PostCandidate → proto ScoredPost                      │   │
└──────────────────────────┬─────────────────────────────────────────┼───┘
                           ▼                                         │
┌────────────────────────────────────────────────────────────────────┴───┐
│ candidate_pipeline.rs:88   CandidatePipeline::execute                  │
│  Drives every PhoenixCandidatePipeline stage in order                  │
└────────────────────────────────────────────────────────────────────────┘
```

## Authoritative pipeline stage order

`candidate_pipeline.rs:88-137` defines the exact sequence. **Verified line-by-line against the source.**

```
                            ┌─────────────────────────┐
                            │  1. QueryHydrators      │  parallel via join_all
                            │     (15 in ScoredPosts) │  cp.rs:202-218
                            │                         │  failures: error logged + swallowed
                            └────────────┬────────────┘
                                         ▼
                            ┌─────────────────────────┐
                            │  2. DependentQuery      │  parallel via join_all
                            │     Hydrators           │  cp.rs:229-248
                            │     (0 in ScoredPosts)  │  ← Phoenix pipeline does NOT
                            │                         │    override the default empty []
                            └────────────┬────────────┘
                                         ▼
                            ┌─────────────────────────┐
                            │  3. Sources             │  parallel via join_all
                            │     (6 in ScoredPosts)  │  cp.rs:257-272
                            │                         │  failures: flatten() drops Err
                            │                         │            (do NOT fail request)
                            └────────────┬────────────┘
                                         ▼
                            ┌─────────────────────────┐
                            │  4. Hydrators           │  parallel via join_all
                            │     (10 in ScoredPosts) │  cp.rs:280-319
                            │                         │  LENGTH-PRESERVING: failures
                            │                         │  skip update; entry retained
                            └────────────┬────────────┘
                                         ▼
                            ┌─────────────────────────┐
                            │  5. Filters             │  SEQUENTIAL
                            │     (14 in ScoredPosts) │  cp.rs:331-386
                            │                         │  ONLY stage that explicitly
                            │                         │  drops entries (kept/removed)
                            └────────────┬────────────┘
                                         ▼
                            ┌─────────────────────────┐
                            │  6. Scorers             │  SEQUENTIAL
                            │     (3 in ScoredPosts:  │  cp.rs:394-404
                            │     PhoenixScorer,      │  LENGTH-PRESERVING: failures
                            │     RankingScorer,      │  skip update; entry retained
                            │     VMRanker)           │
                            └────────────┬────────────┘
                                         ▼
                            ┌─────────────────────────┐
                            │  7. Selector            │  Single sync call
                            │     (TopKScoreSelector) │  cp.rs:407-416
                            │                         │  Truncates to TOP_K
                            │                         │  by candidate.score desc
                            └────────────┬────────────┘
                                         ▼
                            ┌─────────────────────────┐
                            │  8. PostSelection       │  parallel via join_all
                            │     Hydrators           │  cp.rs:107-109
                            │     (6 in ScoredPosts)  │  e.g. brand-safety, VF, ads
                            └────────────┬────────────┘
                                         ▼
                            ┌─────────────────────────┐
                            │  9. PostSelection       │  SEQUENTIAL
                            │     Filters             │  cp.rs:111-113
                            │     (3 in ScoredPosts)  │  Final safety pass
                            └────────────┬────────────┘
                                         ▼
                            ┌─────────────────────────┐
                            │ 10. Result-size         │  cp.rs:115-117
                            │     truncation          │  split_off(result_size())
                            │                         │  → excess to non_selected
                            └────────────┬────────────┘
                                         ▼
                            ┌─────────────────────────┐
                            │ 11. Finalize (no-op)    │  cp.rs:119 / trait :86
                            │                         │  sees post-truncation set
                            └────────────┬────────────┘
                                         ▼
                            ┌─────────────────────────┐
                            │ 12. SideEffects         │  tokio::spawn + join_all
                            │     (6 in ScoredPosts)  │  cp.rs:419-428
                            │                         │  Fire-and-forget
                            │                         │  Errors discarded
                            └─────────────────────────┘
```

### Failure semantics summary

| Stage | On per-entry/component failure | Drops entries? |
|-------|--------------------------------|----------------|
| QueryHydrator | Logged + swallowed in `update()` merge | No |
| DependentQueryHydrator | Same as QueryHydrator | No |
| Source | Whole source's `Err` is **flattened away** (skipped). Other sources unaffected. | No (per-entry); yes (per-source) |
| Hydrator | Returns `Vec<Result<C, String>>` same length as input. Length mismatch ⇒ `vec![Err(msg); expected_len]`. `update_all` applies only `Ok`. | **No** |
| Filter | Partitions into `kept` / `removed`. The only stage that drops entries by predicate. | **Yes** |
| Scorer | Same length-preserving semantics as Hydrator. | **No** |
| Selector | `split_off` into `non_selected`. Final truncation by `result_size()`. | Yes (final truncation only) |
| SideEffect | Spawned + joined; results discarded. | N/A |

**There is no global pipeline timeout and no circuit breaker.** Only `PhoenixScorer` has a retry-like fallback (egress sidecar → direct Phoenix client) — see `phoenix_scorer.rs:86-98`.

## ForYouFeedService composition

```
   Client
     │  ForYouFeedQuery
     ▼
┌──────────────────────────────────────────────────────────────────────────┐
│ home-mixer/server.rs:270…   ForYouFeedService                            │
│  • get_for_you_feed         endpoint = "for_you_feed"                    │
│  • get_for_you_feed_urt     endpoint = "for_you_feed_urt"                │
└──────────────────────────┬───────────────────────────────────────────────┘
                           ▼
┌──────────────────────────────────────────────────────────────────────────┐
│ for_you_candidate_pipeline.rs:48-278   ForYouCandidatePipeline           │
│                                                                          │
│  QueryHydrators (2): ServedHistoryQueryHydrator,                         │
│                      PastRequestTimestampsQueryHydrator                  │
│                                                                          │
│  Sources (5):     ScoredPostsSource  ◀── calls                           │
│                      scored_posts_server.run_pipeline(query.clone())     │
│                      which runs the entire PhoenixCandidatePipeline ABOVE│
│                   AdsSource                                              │
│                   WhoToFollowSource                                      │
│                   PromptsSource                                          │
│                   PushToHomeSource                                       │
│                                                                          │
│  Hydrators:       (empty)                                                │
│  Filters:         (empty)                                                │
│  Scorers:         (empty)                                                │
│                                                                          │
│  Selector:        BlenderSelector                                        │
│                       │                                                  │
│                       └─► partition by type (posts / ads / wtf /         │
│                              prompts / push_to_home), blend ads via      │
│                              SafeGapAdsBlender or PartitionOrganic,      │
│                              insert prompts and WTF at fixed positions,  │
│                              pin push-to-home at position 0              │
│                                                                          │
│  SideEffects (8): AdsInjectionLogging                                    │
│                   PublishSeenIdsToKafka                                  │
│                   ServedCandidatesKafka                                  │
│                   ClientEventsKafka                                      │
│                   ForYouResponseStats                                    │
│                   UpdatePastRequestTimestamps                            │
│                   UpdateServedHistory                                    │
│                   TruncateServedHistory                                  │
└──────────────────────────────────────────────────────────────────────────┘
```

**Key insight:** ForYou is its own `CandidatePipeline` AND it wraps the ScoredPosts pipeline via `ScoredPostsSource` as its first source. ScoredPosts produces ranked post candidates; ForYou interleaves them with ads / WTF / prompts / push-to-home modules.

## Backpressure on the request path

There is **no centralized semaphore on home-mixer's fanout**. Backpressure lives at the boundary services:

- **Thunder**: `try_acquire()` on a request semaphore — returns `RESOURCE_EXHAUSTED` on overload (`thunder_service.rs:160-170`).
- **Thunder Kafka ingest**: bound-3 semaphore reserves CPU for serving during init burst (`tweet_events_listener_v2.rs:56`, applied at `:214-219` only after catchup).
- **Grox dispatcher**: `max_in_flight` gauge in `_fill_loop` (`dispatcher.py:251`).

## Local timeouts on the request path

| Location | Timeout |
|----------|---------|
| Gizmoduck viewer data | 200 ms (`server.rs:177-187`) |
| Cached Redis GET | 300 ms (`cached_posts_query_hydrator.rs:12`) |
| Followed Grok topics | 300 ms (`followed_grok_topics_query_hydrator.rs:15`) |
| Followed starter packs | 300 ms (`followed_starter_packs_query_hydrator.rs:11`) |
| Quote video duration | 200 ms (`quote_hydrator.rs:39`) |
| VF safety labels | 500 ms (`phoenix_candidate_pipeline.rs:540`) |
| Max gRPC connection age | 300 s (`main.rs:56`) |
| Phoenix scorer retry-like fallback | Egress sidecar → direct client on failure (`phoenix_scorer.rs:86-98`) |

---

Next: dig into **[components/candidate-pipeline.md](components/candidate-pipeline.md)** for the trait framework, or **[REPORT.md](REPORT.md)** for the full single-file report.
