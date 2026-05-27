# Component Deep-Dive — `candidate-pipeline` (Rust)

A generic, trait-based async pipeline framework. Defines the abstract execution model; knows nothing about posts, users, or any specific backend.

## Trait inventory

There are **7 component traits** plus 4 framework traits/types.

```
                       ┌──────────────────────────────────┐
                       │  CandidatePipeline<Q, C>         │  (top-level trait)
                       │  candidate_pipeline.rs:68-137    │
                       │  - query_hydrators()             │
                       │  - dependent_query_hydrators()   │  ◀── reuses QueryHydrator
                       │                                  │      (default = &[])
                       │  - sources()                     │
                       │  - hydrators()                   │
                       │  - filters()                     │
                       │  - scorers()                     │
                       │  - selector()                    │
                       │  - post_selection_hydrators()    │  ◀── reuses Hydrator
                       │  - post_selection_filters()      │  ◀── reuses Filter
                       │  - side_effects()                │
                       │  - result_size()                 │
                       │  - finalize() (default: no-op)   │
                       └──────────────────────────────────┘
                                       │
                  ┌────────────────────┼────────────────────┐
                  ▼                    ▼                    ▼
        ┌────────────────┐    ┌────────────────┐   ┌────────────────┐
        │ ASYNC          │    │ SYNC           │   │ ASYNC (spawn)  │
        ├────────────────┤    ├────────────────┤   ├────────────────┤
        │ Source<Q,C>    │    │ Filter<Q,C>    │   │ SideEffect<Q,C>│
        │ QueryHydrator  │    │ Selector<Q,C>  │   │                │
        │ Hydrator<Q,C>  │    │                │   │                │
        │ CachedHydrator │    │                │   │                │
        │ Scorer<Q,C>    │    │                │   │                │
        └────────────────┘    └────────────────┘   └────────────────┘
```

### Async trait signatures

```rust
// source.rs:8-39
#[async_trait]
pub trait Source<Q, C>: Any + Send + Sync {
    fn enable(&self, _query: &Q) -> bool { true }            // default
    async fn run(&self, query: &Q) -> Result<Vec<C>, String>; // instrumented wrapper
    async fn source(&self, query: &Q) -> Result<Vec<C>, String>; // implementor's hook
    fn name(&self) -> &'static str { /* short_type_name */ }
}

// query_hydrator.rs:8-41
#[async_trait]
pub trait QueryHydrator<Q>: Any + Send + Sync {
    fn enable(&self, _query: &Q) -> bool { true }
    async fn run(&self, query: &Q) -> Result<Q, String>;
    async fn hydrate(&self, query: &Q) -> Result<Q, String>;
    fn update(&self, query: &mut Q, hydrated: Q);
    fn name(&self) -> &'static str { /* short_type_name */ }
}

// hydrator.rs:10-67
#[async_trait]
pub trait Hydrator<Q, C>: Any + Send + Sync {
    fn enable(&self, _query: &Q) -> bool { true }
    async fn hydrate(&self, query: &Q, candidates: &[C]) -> Vec<Result<C, String>>;
    async fn run(&self, query: &Q, candidates: &[C]) -> Vec<Result<C, String>>;
    fn update(&self, candidate: &mut C, hydrated: C);
    fn update_all(&self, candidates: &mut [C], hydrated: Vec<Result<C, String>>);
    fn name(&self) -> &'static str { /* short_type_name */ }
}

// scorer.rs:8-65
#[async_trait]
pub trait Scorer<Q, C>: Send + Sync {
    fn enable(&self, _query: &Q) -> bool { true }
    async fn run(&self, query: &Q, candidates: &[C]) -> Vec<Result<C, String>>;
    async fn score(&self, query: &Q, candidates: &[C]) -> Vec<Result<C, String>>;
    fn update(&self, candidate: &mut C, scored: C);
    fn update_all(&self, candidates: &mut [C], scored: Vec<Result<C, String>>);
    fn name(&self) -> &'static str { /* short_type_name */ }
}

// side_effect.rs:16-37
#[async_trait]
pub trait SideEffect<Q, C>: Send + Sync {
    fn enable(&self, _query: Arc<Q>) -> bool { true }   // NOTE: Arc<Q>, not &Q
    async fn run(&self, input: Arc<SideEffectInput<Q, C>>) -> Result<(), String>;
    async fn side_effect(&self, input: Arc<SideEffectInput<Q, C>>) -> Result<(), String>;
    fn name(&self) -> &'static str { /* short_type_name */ }
}
```

### Sync trait signatures

```rust
// filter.rs:16-70
pub trait Filter<Q, C>: Any + Send + Sync {
    fn enable(&self, _query: &Q) -> bool { true }
    fn run(&self, query: &Q, candidates: Vec<C>) -> FilterResult<C>;
    fn filter(&self, query: &Q, candidates: Vec<C>) -> FilterResult<C>;
    fn name(&self) -> &'static str { /* short_type_name */ }
    fn stat(&self, result: &FilterResult<C>);  // default emits kept/removed metric
}

// selector.rs:21-85
pub trait Selector<Q, C>: Send + Sync {
    fn enable(&self, _query: &Q) -> bool { true }
    fn run(&self, query: &Q, candidates: Vec<C>) -> SelectResult<C>;
    fn select(&self, _query: &Q, candidates: Vec<C>) -> SelectResult<C>;  // default sort+truncate
    fn score(&self, candidate: &C) -> f64;
    fn sort(&self, candidates: Vec<C>) -> Vec<C>;  // default sorts by score desc
    fn size(&self) -> Option<usize> { None }
    fn name(&self) -> &'static str { /* short_type_name */ }
}
```

### CachedHydrator (special)

```rust
// hydrator.rs:78-115
#[async_trait]
pub trait CachedHydrator<Q, C>: Any + Send + Sync {
    type CacheKey: Eq + Hash + Send + Sync + 'static;
    type CacheValue: Clone + Send + Sync + 'static;

    fn enable(&self, _query: &Q) -> bool { true }
    fn cache_store(&self) -> &dyn CacheStore<Self::CacheKey, Self::CacheValue>;
    fn cache_key(&self, candidate: &C) -> Self::CacheKey;
    fn cache_value(&self, hydrated: &C) -> Self::CacheValue;
    fn hydrate_from_cache(&self, value: Self::CacheValue) -> C;
    async fn hydrate_from_client(&self, query: &Q, candidates: &[C]) -> Vec<Result<C, String>>;
    fn update(&self, candidate: &mut C, hydrated: C);
    fn name(&self) -> &'static str { /* short_type_name */ }
    fn stat_cache(&self, cache_hits: usize, cache_misses: usize);
}
```

A blanket `impl<T> Hydrator<Q, C> for T where T: CachedHydrator<Q, C>` (hydrator.rs:118-189) automatically makes every CachedHydrator a regular Hydrator, with cache-aside logic and per-key hit/miss metrics emitted on `{name}.cache`.

## Framework traits

```rust
// candidate_pipeline.rs:59-62
pub trait PipelineQuery: Clone + Send + Sync + 'static {
    fn params(&self) -> &xai_feature_switches::Params;
    fn decider(&self) -> Option<&xai_decider::Decider>;
}

// candidate_pipeline.rs:64-65
pub trait PipelineCandidate: Clone + Send + Sync + 'static {}
impl<T> PipelineCandidate for T where T: Clone + Send + Sync + 'static {}
```

This is the integration point with `xai_feature_switches` and `xai_decider`. Every component receives `query` and can call `query.params()` / `query.decider()` from inside its `enable()` hook to gate itself dynamically.

## Pipeline stage execution

Every `CandidatePipeline::execute()` call (`candidate_pipeline.rs:88-137`) runs the same 12 steps:

| # | Stage | Method | Parallelism | File:Line |
|---|-------|--------|-------------|-----------|
| 1 | QueryHydrator | `hydrate_query()` | **Parallel** (`join_all`) | cp.rs:202-218 |
| 2 | DependentQueryHydrator | `hydrate_dependent_query()` | **Parallel** (`join_all`) | cp.rs:229-248 |
| 3 | Source | `fetch_candidates()` | **Parallel** (`join_all`) | cp.rs:257-272 |
| 4 | Hydrator | `hydrate()` | **Parallel** (`join_all`) | cp.rs:280-319 |
| 5 | Filter | `filter()` | **Sequential** (`for` loop) | cp.rs:331-386 |
| 6 | Scorer | `score()` | **Sequential** | cp.rs:394-404 |
| 7 | Selector | `select()` | Once | cp.rs:407-416 |
| 8 | PostSelectionHydrator | `hydrate_post_selection()` | **Parallel** (`join_all`) | cp.rs:107-109 |
| 9 | PostSelectionFilter | `filter_post_selection()` | **Sequential** | cp.rs:111-113 |
| 10 | Result-size truncation | `split_off(result_size())` | Once | cp.rs:115-117 |
| 11 | Finalize | `finalize()` | Once (default no-op) | cp.rs:119 |
| 12 | SideEffect | `run_side_effects()` | `tokio::spawn` + `join_all` | cp.rs:419-428 |

> **Order note:** Truncation runs *before* `finalize()`. The framework calls `split_off(self.result_size().min(final.len()))` at cp.rs:115-117, then `finalize(&mut final)` at cp.rs:119. `finalize` sees the post-truncation, final set.

## Failure semantics (detailed)

### Source — silent per-source drop

```rust
// candidate_pipeline.rs:262-266 (paraphrased)
let results = join_all(source_futures).await;
let merged = results.into_iter().flatten();
//                              ^^^^^^^^^  flatten() drops Err entirely
```

If a Source returns `Err(msg)`, it contributes **zero candidates** — but the pipeline does not fail. Other sources are unaffected. There is no per-source retry.

### Hydrator / Scorer — length-preserving

```rust
// hydrator.rs:30-48, scorer.rs:21-39
async fn run(&self, query: &Q, candidates: &[C]) -> Vec<Result<C, String>> {
    let results = self.hydrate(query, candidates).await;
    if results.len() != candidates.len() {
        // length mismatch ⇒ fill with errors for ALL candidates
        return vec![Err("hydrator length mismatch".into()); candidates.len()];
    }
    results
}
```

`update_all` (default impl) skips `Err` entries — the candidate is preserved unmodified, NOT dropped. This means: **a failed hydration/scoring leaves a hole in the field but the candidate continues through the pipeline.**

### Filter — explicit partition

The only stage allowed to drop entries. Returns `FilterResult { kept, removed }`. Removed candidates are accumulated into `PipelineResult.filtered_candidates` (visible in debug output) but never re-enter the pipeline.

### SideEffect — fire-and-forget

```rust
// candidate_pipeline.rs:419-428 (paraphrased)
tokio::spawn(async move {
    let _ = join_all(futures).await;  // result discarded
});
```

Side-effect errors are never propagated. The pipeline returns to the caller as soon as the spawn completes (immediate).

## Result containers

```rust
// candidate_pipeline.rs:40-58
pub struct PipelineResult<Q, C> {
    pub retrieved_candidates: Vec<C>,   // From sources, AFTER hydration but BEFORE filtering
    //                                  // (set from hydrated_candidates at cp.rs:132)
    pub filtered_candidates: Vec<C>,    // Cumulative removed from filter + post_selection_filter
    pub selected_candidates: Vec<C>,    // Final result after all stages + truncation + finalize
    pub query: Arc<Q>,                  // Hydrated query
}

impl PipelineResult<Q, C> {
    pub fn empty() -> Self where Q: Default { /* … */ }  // used by test-user short-circuit
}

// filter.rs:10-13
pub struct FilterResult<C> {
    pub kept: Vec<C>,
    pub removed: Vec<C>,
}

// selector.rs:6-19
pub struct SelectResult<C> {
    pub selected: Vec<C>,
    pub non_selected: Vec<C>,
}

// side_effect.rs:9-14
pub struct SideEffectInput<Q, C> {
    pub query: Arc<Q>,
    pub selected_candidates: Vec<C>,
    pub non_selected_candidates: Vec<C>,
}
```

## Observability

### Spans

Every stage is wrapped in `#[tracing::instrument]` with stage-specific fields:

| Span | Recorded fields |
|------|-----------------|
| `query_hydrators` / `dependent_query_hydrators` | `total_count`, `enabled_count`, `disabled` (csv of names) |
| `sources` | `total_count`, `enabled_count`, `disabled`, `candidate_count` |
| `hydrators` / `post_selection_hydrators` | `total_count`, `enabled_count`, `disabled` |
| `filters` / `post_selection_filters` | `total_count`, `enabled_count`, `disabled`, `input_count`, `kept_count`, `removed_count`, `filter_rate` |
| `scorers` | `total_count`, `enabled_count`, `disabled` |
| `selector` | `name`, `input_count`, `selected_count`, `non_selected_count` |
| `source` / `hydrator` / `scorer` / `query_hydrator` (per-component) | `name` |

### Metrics (via `#[xai_stats_macro::receive_stats]`)

| Trait | Latency / size bucket |
|-------|------------------------|
| Source | `size = Bucket500To1000` |
| Hydrator | `latency = Bucket50To500`, `size = Bucket500To2500` |
| Filter | `latency = Bucket0To50` |
| Selector | `latency = Bucket0To50`, `size = Bucket0To50` |
| CandidatePipeline (`execute`) | `latency = Bucket500To2500` |
| Filter `{name}.run` | scopes `kept` / `removed` (filter.rs:62, 66) |
| CachedHydrator `{name}.cache` | scopes `cache_hit` / `cache_miss` (hydrator.rs:108-111) |
| Pipeline finalization `{name}.execute` | `result_size`, `result_empty` (cp.rs:482, 489) |

## Special patterns

### `enable()` is the universal feature flag hook

Every component declares `enable(&self, query)` (default `true`). The signature is `&Q` for most traits but **`Arc<Q>`** for `SideEffect` (`side_effect.rs:23`). The framework calls it twice per stage — once during the `record_enabled_components` pre-pass for span field collection (cp.rs:205-206), and again when actually invoking the component. If `false`, the component is skipped *and* its name is added to the span's `disabled` field. Combined with `query.params()` access, this is the lever for all A/B tests and ramp-ups.

### Two-phase query hydration

`query_hydrators()` (stage 1) runs first; `dependent_query_hydrators()` (stage 2) runs after, so its hydrators can read fields populated in stage 1. **Both slots reuse the same `QueryHydrator` trait** — `dependent_query_hydrators()` is not a separate trait. The default impl returns `&[]`.

### Post-selection slots reuse Hydrator/Filter

`post_selection_hydrators()` returns `&[Box<dyn Hydrator<Q, C>>]`, `post_selection_filters()` returns `&[Box<dyn Filter<Q, C>>]`. They're regular Hydrators/Filters, just invoked on the small post-selection candidate set so expensive ops (VF, brand-safety) only run on what's actually returned.

### Two truncation points

1. `Selector::size()` — sorts + splits during stage 7
2. `result_size()` — explicit `split_off` at stage 11, removing anything that survived post-selection filtering above the limit

Both move excess candidates into `non_selected_candidates`, which is then passed to side effects (so side effects see the full superset).

### Side effects = spawned + dropped

`tokio::spawn` at cp.rs:421; the spawned task `join_all`s all side effects and the entire `Result` is dropped. Query and candidates are wrapped in `Arc<SideEffectInput<Q, C>>` before spawn so the pipeline can return without waiting.

### `PipelineResult::empty()` fast path

Used by `scored_posts_server.rs:45-49` for test users to return immediately without running the pipeline.

## File map

| File | LOC | Defines |
|------|----:|---------|
| `lib.rs` | 9 | module exports |
| `candidate_pipeline.rs` | 493 | `CandidatePipeline`, `PipelineQuery`, `PipelineCandidate`, `PipelineResult`, `PipelineStage`, `PipelineComponents` |
| `source.rs` | 39 | `Source` |
| `query_hydrator.rs` | 41 | `QueryHydrator` |
| `hydrator.rs` | 189 | `Hydrator`, `CachedHydrator`, `CacheStore` |
| `filter.rs` | 70 | `Filter`, `FilterResult` |
| `scorer.rs` | 65 | `Scorer` |
| `selector.rs` | 85 | `Selector`, `SelectResult` |
| `side_effect.rs` | 37 | `SideEffect`, `SideEffectInput` |
| `util.rs` | 3 | `short_type_name` |
