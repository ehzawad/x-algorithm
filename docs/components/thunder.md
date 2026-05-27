# Component Deep-Dive — `thunder` (Rust)

X's in-memory in-network post cache. Listens on the `IN_NETWORK_EVENTS` Kafka topic and exposes a single gRPC endpoint, `GetInNetworkPosts`.

## Storage layout — 5 DashMaps (verified)

```rust
// thunder/posts/post_store.rs:38-53
pub struct PostStore {
    posts: Arc<DashMap<i64, LightPost>>,                              // Map 1
    original_posts_by_user: Arc<DashMap<i64, VecDeque<TinyPost>>>,    // Map 2
    secondary_posts_by_user: Arc<DashMap<i64, VecDeque<TinyPost>>>,   // Map 3
    video_posts_by_user: Arc<DashMap<i64, VecDeque<TinyPost>>>,       // Map 4
    deleted_posts: Arc<DashMap<i64, bool>>,                           // Map 5
    retention_seconds: u64,
    request_timeout: Duration,
}
```

```
                           ┌───────────────────────────┐
                           │       PostStore           │
                           │   post_store.rs:38-53     │
                           └────────────┬──────────────┘
                                        │  Arc-shared, single global instance
                                        │  No app-level sharding — DashMap provides
                                        │  per-bucket locking; that's it.
            ┌───────────────────────────┼───────────────────────────┐
            │           │               │              │            │
            ▼           ▼               ▼              ▼            ▼
   ┌────────────┐ ┌──────────────┐ ┌─────────────┐ ┌──────────┐ ┌──────────┐
   │ posts      │ │ original_    │ │ secondary_  │ │ video_   │ │ deleted_ │
   │            │ │ posts_by_    │ │ posts_by_   │ │ posts_by │ │ posts    │
   │ key:       │ │ user         │ │ user        │ │ _user    │ │          │
   │ i64        │ │              │ │             │ │          │ │ key: i64 │
   │ (post_id)  │ │ key: i64     │ │ key: i64    │ │ key: i64 │ │  (post)  │
   │            │ │ (author_id)  │ │ (author_id) │ │ (author_)│ │          │
   │ value:     │ │              │ │             │ │          │ │ value:   │
   │ LightPost  │ │ value:       │ │ value:      │ │ value:   │ │ bool     │
   │            │ │ VecDeque<    │ │ VecDeque<   │ │ VecDeque │ │  (tomb-  │
   │            │ │ TinyPost>    │ │ TinyPost>   │ │ <TinyP.> │ │  stone)  │
   │            │ │              │ │             │ │          │ │          │
   │ Primary    │ │ Originals    │ │ Replies +   │ │ Video-   │ │ Prevents │
   │ store      │ │ per author   │ │ retweets    │ │ eligible │ │ zombie   │
   │            │ │              │ │ per author  │ │ subset   │ │ serves   │
   └────────────┘ └──────────────┘ └──────────────┘ └──────────┘ └──────────┘
```

`TinyPost` is 16 bytes: `(post_id: i64, created_at: i64)` (`post_store.rs:19-34`). Heavy `LightPost` data is stored once in `posts`; the per-user timelines hold only tiny pointers.

## Kafka ingestion

```
   ┌───────────────────────────────────────────────────────────────┐
   │  IN_NETWORK_EVENTS Kafka topic (name redacted in OSS release) │
   │  Message variants: TweetCreateEvent, TweetDeleteEvent         │
   │  Consumer config: max_partition_fetch_bytes=100MB             │
   │                   unique consumer group per instance (UUID)   │
   │  (thunder/kafka_utils.rs:38-57)                               │
   └────────────────────────────┬──────────────────────────────────┘
                                ▼
   ┌───────────────────────────────────────────────────────────────┐
   │  tweet_events_listener_v2.rs:169-249                          │
   │  • poll(batch_size) on consumer (write-locked)                │
   │  • deserialize_batch() :119-167                               │
   │  • Bound-3 semaphore (line 56) — acquired ONLY                │
   │    AFTER init catchup completes (:214-219)                    │
   │    Caps post-catchup blocking-ingest work to 3 threads so     │
   │    CPU stays available for request serving. During the        │
   │    initial catchup burst the permit is None, so threads       │
   │    ingest at full parallelism.                                │
   └────────────────────────────┬──────────────────────────────────┘
                                ▼
   ┌───────────────────────────────────────────────────────────────┐
   │  post_store.insert_posts(light_posts)        (:115-168)       │
   │  post_store.mark_as_deleted(delete_tweets)   (:69-83)         │
   │                                                               │
   │  For each LightPost:                                          │
   │    1. posts.insert(post_id, post)               (:127)        │
   │    2. tiny = TinyPost(post_id, created_at)                    │
   │    3. if original:                                            │
   │         original_posts_by_user[author_id].push(tiny) (:139)   │
   │       else:                                                   │
   │         secondary_posts_by_user[author_id].push(tiny)(:143)   │
   │    4. video_eligible = post.has_video                         │
   │         + retweet inheritance: if is_retweet AND src has      │
   │           video AND src is not reply (:150-156)               │
   │         + replies forced false (:158-160)                     │
   │       if video_eligible: video_posts_by_user[a].push(tiny)    │
   │                                                               │
   │  For each delete:                                             │
   │    1. posts.remove(post_id)                     (:71)         │
   │    2. deleted_posts.insert(post_id, true)       (:72)         │
   │    3. audit trail entry in original_posts_by_user             │
   │       [DELETE_EVENT_KEY]                        (:74-81)      │
   └───────────────────────────────────────────────────────────────┘
```

### Catchup → serving transition

```
   ┌─────────────────────────────────────────────────────────────┐
   │  All threads spawn at startup; poll Kafka aggressively.     │
   │  Semaphore NOT acquired (init_data_downloaded = false)      │
   │  → max parallelism for fast ingestion.                      │
   │                                                             │
   │  Per-thread catchup check (every batch, :189-204):          │
   │    total_lag = sum(partition lags)                          │
   │    if total_lag < num_partitions * batch_size:              │
   │       init_data_downloaded = true                           │
   │       signal main thread via mpsc channel                   │
   │                                                             │
   │  main.rs:73-75 waits for ALL kafka_num_threads to signal,   │
   │  then calls finalize_init() which:                          │
   │    • sort_all_user_posts() — sort timelines by created_at   │
   │    • trim_old_posts()      — initial trim pass              │
   │    • clean deleted_posts   — remove any zombies             │
   │                                                             │
   │  Then http_server.set_readiness(true) → accepts requests.   │
   │                                                             │
   │  From this point: semaphore IS acquired per batch (:216)    │
   │    Max 3 concurrent blocking insert tasks → leaves CPU      │
   │    for request handling under hot load.                     │
   └─────────────────────────────────────────────────────────────┘
```

## Retention / auto-trim

```rust
// main.rs:84-89
Arc::clone(&post_store).start_auto_trim(2);  // Run every 2 minutes
```

```rust
// post_store.rs:521-526
impl Default for PostStore {
    fn default() -> Self {
        Self::new(2 * 24 * 60 * 60, 0)  // 172800 s = 2 days
    }
}
```

`trim_old_posts()` (`post_store.rs:409-476`; body inside `spawn_blocking` starts at :417):

```rust
// Spawned via spawn_blocking (line 417)
// For each (author_id, VecDeque<TinyPost>) entry in each of the 3 timeline maps:
//   while user_posts.front().created_at + retention < now:
//     pop_front()
//     posts.remove(post_id)
//     (audit special case for DELETE_EVENT_KEY)
//
// Shrink capacity (line 451-453):
//   if user_posts.capacity() > user_posts.len() * 2:
//     user_posts.shrink_to(user_posts.len() * 1.5)
//
// Remove empty user entries (atomic remove_if).
```

Configurable via the `post_retention_seconds` argument (`main.rs:22`). The exact CLI flag spelling lives in `thunder/args.rs`, which is not in this OSS checkout — the field name is the closest verifiable evidence.

## gRPC serving — `GetInNetworkPosts`

```rust
// thunder_service.rs:154-330
// Request:  { user_id, following_user_ids, exclude_tweet_ids, is_video_request, max_results, debug }
// Response: { posts: Vec<LightPost> }
```

### Admission control

```rust
// thunder_service.rs:160-171
let _permit = match self.request_semaphore.try_acquire() {
    Ok(p) => { IN_FLIGHT_REQUESTS.inc(); p },
    Err(_) => {
        REJECTED_REQUESTS.inc();
        return Err(Status::resource_exhausted("Server at capacity, please retry"));
    }
};
```

Non-blocking; on capacity miss, return `RESOURCE_EXHAUSTED` immediately. The semaphore size is configurable (`max_concurrent_requests` arg).

### CPU-bound retrieval

```rust
// thunder_service.rs:274-314
let proto_posts = tokio::task::spawn_blocking(move || {
    // build exclude_tweet_ids HashSet
    let all_posts = if req.is_video_request {
        post_store.get_videos_by_users(...)
    } else {
        post_store.get_all_posts_by_users(...)
    };
    // analyze, score_recent, return
})
.await?;
```

Off the tokio worker thread — sorting 1000s of posts is genuinely CPU-bound.

### The lock-free read pattern

```rust
// post_store.rs:260-285
if let Some(user_posts_ref) = posts_map.get(user_id) {
    let user_posts = user_posts_ref.value();
    let tiny_posts_iter = user_posts
        .iter()
        .rev()                                                  // newest first
        .filter(|p| !exclude_tweet_ids.contains(&p.post_id))
        .take(MAX_TINY_POSTS_PER_USER_SCAN);                   // soft scan limit

    // We copy the value immediately to release the read lock and avoid
    // potential deadlock when acquiring nested read locks while a writer
    // is waiting.                                            ◀── verbatim comment
    let light_post_iter_1 = tiny_posts_iter
        .filter_map(|t| self.posts.get(&t.post_id).map(|r| *r.value()));
    //                                            ^^^^^^^^^^^  COPY here
    //                                                         releases inner read lock
}
```

This pattern is repeated for each requested user. Without it, holding the per-user timeline read lock while acquiring per-post read locks could deadlock against a writer.

### Tombstone double-check

```rust
// post_store.rs:278-285
let light_post_iter = light_post_iter_1.filter(|post| {
    if self.deleted_posts.get(&post.post_id).is_some() {
        POST_STORE_DELETED_POSTS_FILTERED.inc();
        false
    } else {
        true
    }
});
```

Defensive: even though deletes remove from `posts` at write time, a race between insert and delete events could theoretically leave a zombie post. The tombstone check eliminates that.

### Reply conversation filtering

For the secondary (replies+retweets) timeline, replies are only included if either:
- The replied-to post is itself an original (i.e., reply directly to followed user's original), OR
- The reply is part of a conversation thread whose root is the conversation_id AND the in_reply_to_user is followed.

(`post_store.rs:287-315`)

This prevents random replies to followed users (deep in unrelated threads) from appearing in the feed.

## Strato fallback (debug only)

If `following_user_ids` is empty **and** `req.debug == true`, fetch the list from Strato (`thunder_service.rs:197-228`). Production traffic never hits this path — clients always pass a pre-fetched list. Useful for hand-rolled investigations against `grpcurl --plaintext ... --debug`.

## Optimizations recap

| Pattern | Location | Benefit |
|---------|----------|---------|
| Lock-free copy-on-read | `post_store.rs:273-276` | Avoids nested-lock deadlock |
| Entry API insertion | `post_store.rs:137-145` | Single DashMap lookup vs. 2 |
| Video inheritance from retweets | `post_store.rs:147-166` | Retweets of videos are discoverable |
| Soft scan limit | `post_store.rs:266-270` | Prevents O(N) for inactive users with huge backlogs |
| Capacity shrinking on trim | `post_store.rs:451-453` | Reclaims VecDeque overallocation |
| Reverse-chronological iteration | `post_store.rs:266` | Newest-first without sorting |
| TinyPost (16-byte) timeline entries | `post_store.rs:19-34` | Lower memory + faster cache lines |

## Metrics

PostStore (`post_store.rs:13-17`):
`POST_STORE_DELETED_POSTS`, `POST_STORE_DELETED_POSTS_FILTERED`, `POST_STORE_ENTITY_COUNT` (labelled gauge), `POST_STORE_POSTS_RETURNED`, `POST_STORE_POSTS_RETURNED_RATIO`, `POST_STORE_REQUESTS`, `POST_STORE_REQUEST_TIMEOUTS`, `POST_STORE_TOTAL_POSTS`, `POST_STORE_USER_COUNT`.

gRPC (`thunder_service.rs:18-25`):
`GET_IN_NETWORK_POSTS_COUNT`, `GET_IN_NETWORK_POSTS_DURATION`, `GET_IN_NETWORK_POSTS_DURATION_WITHOUT_STRATO`, `GET_IN_NETWORK_POSTS_EXCLUDED_SIZE`, `GET_IN_NETWORK_POSTS_FOLLOWING_SIZE`, `GET_IN_NETWORK_POSTS_FOUND_FRESHNESS_SECONDS`, `GET_IN_NETWORK_POSTS_FOUND_POSTS_PER_AUTHOR`, `GET_IN_NETWORK_POSTS_FOUND_REPLY_RATIO`, `GET_IN_NETWORK_POSTS_FOUND_TIME_RANGE_SECONDS`, `GET_IN_NETWORK_POSTS_FOUND_UNIQUE_AUTHORS`, `IN_FLIGHT_REQUESTS`, `REJECTED_REQUESTS`.

Kafka: `KAFKA_MESSAGES_FAILED_PARSE`, `KAFKA_PARTITION_LAG`, `KAFKA_POLL_ERRORS`, `BATCH_PROCESSING_TIME`.
