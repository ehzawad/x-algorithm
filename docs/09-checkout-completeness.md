# 09 — Checkout Completeness

This release is **the architecture, not the production deployable.** Reading the code reveals several modules referenced by source files that are absent from the filesystem (likely XAI-internal infra).

## What you can do with this checkout

- Read and trace the full request lifecycle end-to-end
- Understand the trait framework, pipeline assembly, and stage semantics
- Inspect every Phoenix ML primitive (mask construction, two-tower architecture, hash projections, residual structure)
- Run Phoenix unit tests (`uv run pytest test_recsys_model.py test_recsys_retrieval_model.py` — 34 pass)

## What you **cannot** do

- Build and run the Rust services end-to-end (missing internal config / monitoring crates)
- Re-create the trained Phoenix model (`artifacts/oss-phoenix-artifacts.zip` is an LFS pointer)
- Run grox pipelines (missing config + external infra clients)

This is a **reference release** for understanding the production system's design, not a self-contained runtime.

## Missing modules — exhaustive list

### `grox` (Python)
```
✗ grox.config.config            (TaskGeneratorType, grox_config, KafkaTopicName)
✗ grox.data_loaders.media_processor   (MediaProcessor)
✗ grox.data_loaders.asr_processor     (ASRProcessor)
✗ grox.lm.post_v5
✗ External: monitor.logging, monitor.metrics,
            kafka_cli.producer, strato_http.*
```

### `home-mixer` (Rust)
```
✗ xai_home_mixer::params        (imported in main.rs:6 but not declared in lib.rs:1)
```

### `thunder` (Rust)
```
✗ args, config, metrics, schema, strato_client
  (declared in lib.rs:1 but not in tree)

✗ Constants imported from crate::config (not provided):
    DELETE_EVENT_KEY                special user_id for delete audit trail
    MAX_ORIGINAL_POSTS_PER_AUTHOR   per-user original post cap
    MAX_REPLY_POSTS_PER_AUTHOR      per-user reply cap
    MAX_VIDEO_POSTS_PER_AUTHOR      per-user video cap
    MAX_TINY_POSTS_PER_USER_SCAN    soft scan limit per user
    MAX_INPUT_LIST_SIZE             admission control limit
    MAX_POSTS_TO_RETURN             default max results
    MAX_VIDEOS_TO_RETURN            default max videos
    MIN_VIDEO_DURATION_MS           video eligibility threshold

✗ Kafka topic name redacted in kafka_utils.rs:
    IN_NETWORK_EVENTS_DEST = ""
    IN_NETWORK_EVENTS_TOPIC = ""
```

### `phoenix` (Python)
```
✗ artifacts/oss-phoenix-artifacts.zip is an LFS pointer only
  (~2.9 GB uncompressed; contains model parameters, embedding tables, configs)

✗ No training loops, optimizer state, distributed training,
  or gradient checkpointing infrastructure
```

## Verification approach for documentation

Despite the missing modules, the **public interfaces, traits, and call graphs are complete**:
- Every gRPC service definition is intact
- Every pipeline assembly (component wiring) is visible
- Every Phoenix transformer block / mask / head is implemented
- Every fault-tolerance pattern (retries, semaphores, timeouts) is visible

This lets us produce a faithful technical report on **how the system works** without being able to run it end-to-end.
