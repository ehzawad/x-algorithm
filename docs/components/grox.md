# Component Deep-Dive — `grox` (Python)

Off-path Python DAG executor for ML pipelines (training-time + offline scoring). **Not on the synchronous request path.**

## Core abstractions

```
   ┌────────────────────────────────────────────────────────────┐
   │  Plan (grox/plans/plan.py:16-106)                          │
   │  ────────────────────────────────                          │
   │   • TASKS:              dict[str, Task class]              │
   │   • TASK_DEPENDENCIES:  dict[str, set[str]]  (DAG edges)   │
   │   • REQUIRED_ELIGIBILITY: routing predicate                │
   │   • execute():  asyncio.gather of compiled futures         │
   └────────────────────────────────────────────────────────────┘
                              │ composes
                              ▼
   ┌────────────────────────────────────────────────────────────┐
   │  Task (grox/tasks/task.py:27-150)                          │
   │  ────────────────────────────                              │
   │   • @retry(stop_after_attempt(2), wait_fixed(1)) on exec() │
   │   • should_skip(), should_disable() conditional logic      │
   │   • TaskStopExecution → SKIPPED                            │
   │   • Subclasses: TaskWithUser, TaskWithPost,                │
   │                 TaskWithUserContext,                       │
   │                 TaskWithContentAnalysis                    │
   └────────────────────────────────────────────────────────────┘
                              │ orchestrated by
                              ▼
   ┌────────────────────────────────────────────────────────────┐
   │  Engine (grox/engine.py:25-138)                            │
   │  ────────────────────────────                              │
   │   • Async worker; dedicated Process                        │
   │   • Init: MediaProcessor.start(), ASRProcessor.start()     │
   │   • Polls task queue non-blocking, spawns async tasks      │
   │   • Metrics: processing_time histogram, success/failure    │
   └────────────────────────────────────────────────────────────┘
                              │ fed by
                              ▼
   ┌────────────────────────────────────────────────────────────┐
   │  Dispatcher (grox/dispatcher.py:39-371)                    │
   │  ──────────────────────────────────                        │
   │   • _fill_loop: backpressure via max_in_flight (:251)      │
   │   • _result_loop: retries failed up to max_attempts        │
   │   • Init: @retry(stop=3,                                   │
   │            wait_incrementing(start=1, increment=3, max=9)) │
   │            ⇒ waits 1s, 4s, 7s                              │
   └────────────────────────────────────────────────────────────┘
```

## Execution model

Plans and tasks themselves use **async/await** (no thread pool in the orchestration layer). `Plan.execute()` does `asyncio.gather` on compiled task futures. Skip propagation: if any dependency returns `SKIPPED`, the dependent task is skipped too (`plan.py:89-92`).

> Caveat: blocking work in some data loaders is offloaded to a `ThreadPoolExecutor` (e.g. `grox/data_loaders/kafka_loader.py:10, 26, 105-108` with `max_workers=12`, and `grox/data_loaders/asr_processor.py:99-102` via `run_in_executor`). So "no threadpool" applies to the DAG executor; the data-loading side does use one.

## Output sinks

| Sink class | Goes to | Used by |
|------------|---------|---------|
| `TaskPublishKafka` | Kafka topic `GROX_CONTENT_ANALYSIS` | PlanInitialBanger, PlanSpamComment |
| `TaskPublishUnifiedPostAnnotationsManhattan` | Strato → Manhattan (UPA) | PlanInitialBanger |
| `TaskUpsertTweetBoolMetadataToUnifiedPostAnnotation` | Strato → Manhattan (UPA bool metadata) | PlanPostSafety |
| `TaskWriteMMEmbeddingSinkV5SkipKafkaForReplies` | Manhattan (skips Kafka for replies) | PlanPostEmbeddingV5, PlanPostEmbeddingV5ForReply |
| `TaskWriteSafetyPostAnnotationsResultSink` | Custom safety sink | PlanSafetyPtos |

## 9 defined plans (`plan_master.py:19-29`)

| Plan | Eligibility | Output |
|------|------------|--------|
| `PlanInitialBanger` | BANGER_INITIAL_SCREEN | UPA + Kafka (quality signal) |
| `PlanPostSafety` | POST_SAFETY | UPA (bool metadata, safety screening) |
| `PlanSpamComment` | — | Manhattan + Kafka (reply spam detection) |
| `PlanPostEmbeddingWithSummary` | — | Manhattan (embedding v2) |
| `PlanPostEmbeddingWithSummaryForReply` | — | Manhattan (v2 for replies) |
| `PlanPostEmbeddingV5` | MM_EMB_V5 | Manhattan (multimodal v5; skips Kafka for replies) |
| `PlanPostEmbeddingV5ForReply` | MM_EMB_V5_FOR_REPLY | Manhattan |
| `PlanReplyRanking` | REPLY_RANKING | Manhattan (reply ranking scores) |
| `PlanSafetyPtos` | SAFETY_PTOS | Custom sink (policy-based screening) |

## Fault tolerance — three retry layers

| Layer | Configuration | Effect |
|-------|---------------|--------|
| Task | `@retry(stop_after_attempt(2), wait_fixed(1))` | 2 attempts, 1 s fixed wait (task.py:31) |
| Sink | `@retry(stop_after_attempt(3), wait_chain(wait_fixed(1), wait_fixed(2)))` | 3 attempts, waits 1 s then 2 s (task_write_mm_embedding_sink.py:34) |
| Dispatcher init | `@retry(stop_after_attempt(3), wait_incrementing(start=1, increment=3, max=9))` | 3 attempts ⇒ 2 inter-attempt waits: 1 s, then 4 s (dispatcher.py:75). The `max=9` cap would only matter at attempt 4+, which `stop_after_attempt(3)` never reaches. |

## Caching

`@functools.cache` on `Plan.get_name()` (plan.py:104) and `Task.get_name()` (task.py:88) — memoizes the snake-case name conversion.

## Observability

All paths use OpenTelemetry:

```python
with Metrics.tracer("engine").start_as_current_span("task.root"):
    ...
Metrics.histogram("engine.task.processing_time").record(duration)
Metrics.counter("engine.task.success.count").add(1)
Metrics.counter("engine.task.failed.count").add(1)
```

(`engine.py:57, 62, 73, 85`)

## Task generators (dispatcher routing)

Dispatcher routes from multiple sources (`dispatcher.py:84-198`):

```
PostStreamTaskGenerator
PostStreamRecoveryTaskGenerator
PostStreamTestTaskGenerator
PostStreamDelayedTaskGenerator
PostSafetyStreamTaskGenerator
MinTractionPostStreamForGroxTaskGenerator
MinTractionPostStreamForGroxMultiModalTaskGenerator
PostEmbeddingRequestWithSummaryStreamTaskGenerator
PostEmbeddingV5StreamTaskGenerator
ReplyRankingRecoveryTaskGenerator
SafetyPtosRecoveryStreamTaskGenerator
SafetyPtosDeluxeStreamTaskGenerator
```

## Execution flow

```
Main (async)
├─ Engine.start() → Process(target=Engine.run)
│  └─ Engine._run()
│     ├─ _init_run() [MediaProcessor, ASRProcessor start]
│     ├─ Loop: _poll_task() from queue (non-blocking)
│     └─ asyncio.create_task(_run_task(task))
│        ├─ _process_task(task)
│        │  └─ PlanMaster.exec(task)
│        │     └─ asyncio.gather(*[p.execute(task) for p in ALL_PLANS])
│        │        └─ Plan.execute(task)
│        │           ├─ _eligible(task) check
│        │           └─ asyncio.gather(*[_execute_task(t, ctx, deps)])
│        │              └─ Task.exec(ctx) [@retry(2, 1s)]
│        │                 ├─ should_skip(ctx)
│        │                 └─ _exec(ctx)
│        └─ Put(TaskResult) to resp_queue
│
Dispatcher (async, separate process)
├─ Dispatcher._init_run()
├─ Loop: _fill_loop() [push tasks, max_in_flight backpressure]
└─ _result_loop() [monitor responses, retry failed]
```

## Missing in checkout

- `grox.config.config` (TaskGeneratorType, grox_config, KafkaTopicName)
- `grox.data_loaders.media_processor` (MediaProcessor)
- `grox.data_loaders.asr_processor` (ASRProcessor)
- `grox.lm.post_v5`
- External clients: `monitor.logging`, `monitor.metrics`, `kafka_cli.producer`, `strato_http.*`
