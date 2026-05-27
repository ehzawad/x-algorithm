# Component Deep-Dive — `phoenix` (Python / JAX + Haiku)

The ML brain. A JAX+Haiku transformer derived from Grok-1, adapted for recommendation via two-tower retrieval and candidate-isolation-attention ranking.

## Configuration (mini)

```
emb_size           = 128
num_layers         = 4
num_q_heads        = 4
num_kv_heads       = 4
key_size           = 32
widening_factor    = 2.0
history_seq_len    = 127
candidate_seq_len  = 64
default dtype      = bfloat16
```

## The Grok-1 transformer block

```python
# grok.py:495-520  (sequential, double-normalized residual)
h = inputs

# Attention sub-block
attn_output = MHABlock(...)(layer_norm(h), mask, positions=positions)  # :502
h_attn = attn_output.embeddings
h_attn = layer_norm(h_attn)                                            # :505 (post-norm!)
h += h_attn                                                            # :506

# Dense sub-block
h_dense = base_dense_block(layer_norm(h))                              # :517
h_dense = layer_norm(h_dense)                                          # :519 (post-norm!)
h += h_dense                                                           # :520
```

This is **sequential** (not parallel `h + attn + ffn`) and **double-normalized** — both inputs and outputs of each sub-block are RMSNormed.

### Block diagram

```
        ┌──────────┐
        │   h_in   │
        └────┬─────┘
             │
   ┌─────────┼──────────┐
   │         ▼          │
   │      RMSNorm       │
   │         │          │
   │         ▼          │
   │  ATTN  (Q,K,V w/   │
   │   RoPE applied to  │
   │   Q & K; logit     │
   │   30·tanh(x/30);   │
   │   optional         │
   │   candidate-       │
   │   isolation mask)  │
   │         │          │
   │         ▼          │
   │      RMSNorm       │
   │         │          │
   │         ▼ (h_attn) │
   └────┬────────┬──────┘
        │        │
        ▼ + h_in │
   ┌──────────┐  │
   │ h := h_in│  │
   │   + h_attn  │
   └────┬─────┘  │
        │        │
   ┌────┼────────┼───────┐
   │    ▼        │       │
   │ RMSNorm     │       │
   │    │        │       │
   │    ▼        │       │
   │  GEGLU FFN  │       │
   │  (gelu(W1·x)│       │
   │   * (Wv·x)) │       │
   │   → W2·…    │       │
   │    │        │       │
   │    ▼        │       │
   │ RMSNorm     │       │
   │    │        │       │
   │    ▼(h_ffn) │       │
   └────┬────────┘       │
        │                │
        ▼ + h            │
   ┌──────────┐          │
   │ h := h   │  ◄───────┘
   │   + h_ffn│
   └──────────┘
```

### RMSNorm (`grok.py:186-218`)

```python
# Formula: scale * x * rsqrt(mean(x²) + eps)
# scale initialized to Constant(0)        ◀── critical (grok.py:204)
# axis = -1, eps = 1e-5
```

### RoPE (`grok.py:229-286`)

- `base_exponent = int(1e4)` (line 351)
- Applied to **both Q and K** (lines 352-353)
- Right-anchored variant available for ranking (`right_anchored_rope_positions`, lines 88-109; used when `config.right_anchored_rope=True`, recsys_model.py:649-654)

### FFN — GEGLU, not SwiGLU (`grok.py:453-466`)

```python
h_v  = Linear(ffn_size(...), with_bias=False, name="linear_v")(inputs)
h_w1 = jax.nn.gelu(Linear(ffn_size(...), with_bias=False)(inputs))  # ← GELU
h_dense = Linear(model_size, with_bias=False)(h_w1 * h_v)
```

GEGLU = GELU-gated. The candidate retrieval MLP (`recsys_retrieval_model.py:105`) separately uses `jax.nn.silu` — these are **different activations in different places**, easy to confuse.

### Attention logit clamp (`grok.py:366-368`)

```python
max_attn_val = jnp.array(30.0, dtype=attn_logits.dtype)
attn_logits = max_attn_val * jnp.tanh(attn_logits / max_attn_val)
```

Soft-clamps attention logits to ±30 — prevents softmax saturation in bfloat16.

## Candidate-isolation attention mask

The single critical recsys insight in the model. When ranking a batch of `cand_len` candidates concatenated after a user history, **candidates must not attend to each other** — that would leak ranking info and entangle independent decisions.

```python
# grok.py:39-71  (paraphrased)
causal_mask = jnp.tril(jnp.ones((1, 1, seq_len, seq_len), dtype=dtype))

# Step 2: zero the candidate-candidate block
attn_mask = causal_mask.at[:, :,
    candidate_start_offset:, candidate_start_offset:].set(0)

# Step 3: re-add diagonal so each candidate attends to itself
candidate_indices = jnp.arange(candidate_start_offset, seq_len)
attn_mask = attn_mask.at[:, :, candidate_indices, candidate_indices].set(1)
```

```
   Step 1: Causal lower-triangular              Step 2: Zero candidate-candidate
            user_hist   candidates                       user_hist   candidates
          ┌──────────┬─────────────┐                   ┌──────────┬─────────────┐
   u_hist │ ■ ■ ■    │             │            u_hist │ ■ ■ ■    │             │
          │ ■ ■ ■    │             │                   │ ■ ■ ■    │             │
          │ ■ ■ ■    │             │                   │ ■ ■ ■    │             │
          ├──────────┼─────────────┤                   ├──────────┼─────────────┤
   cands  │ ■ ■ ■    │ ■           │            cands  │ ■ ■ ■    │ . . .       │
          │ ■ ■ ■    │ ■ ■         │                   │ ■ ■ ■    │ . . .       │
          │ ■ ■ ■    │ ■ ■ ■       │                   │ ■ ■ ■    │ . . .       │
          └──────────┴─────────────┘                   └──────────┴─────────────┘

   Step 3: Re-add diagonal (candidate attends to itself)
            user_hist   candidates
          ┌──────────┬─────────────┐
   u_hist │ ■ ■ ■    │             │
          │ ■ ■ ■    │             │
          │ ■ ■ ■    │             │
          ├──────────┼─────────────┤
   cands  │ ■ ■ ■    │ ■ . .       │   ← each candidate attends to user history
          │ ■ ■ ■    │ . ■ .       │     and to itself, nothing else
          │ ■ ■ ■    │ . . ■       │
          └──────────┴─────────────┘
```

Activated when `candidate_start_offset is not None` is passed into `Transformer.__call__` (`grok.py:571`).

## Two-tower retrieval

```
   USER TOWER                                      CANDIDATE TOWER
   recsys_retrieval_model.py:221-291               recsys_retrieval_model.py:47-112

   ┌──────────────────────┐                        ┌─────────────────────────────┐
   │ user history tokens  │                        │ candidate hash embeddings   │
   │ [B, T_hist]          │                        │ [B, C, num_hashes, D]       │
   └──────────┬───────────┘                        └──────────────┬──────────────┘
              │                                                   │
              ▼                                                   ▼
   ┌──────────────────────┐                        ┌─────────────────────────────┐
   │ Phoenix Transformer  │                        │ proj_1: [in_dim, 2D]        │
   │ (same arch as ranker;│                        │           │                 │
   │ standard causal mask)│                        │           ▼                 │
   └──────────┬───────────┘                        │       jax.nn.silu           │
              │                                    │           │                 │
              ▼                                    │           ▼                 │
   ┌──────────────────────┐                        │ proj_2: [2D, D]             │
   │ masked mean pool     │                        │           │                 │
   │ axis=1 → [B, D]      │                        │           ▼                 │
   └──────────┬───────────┘                        │   L2 normalize              │
              │                                    │           │                 │
              ▼                                    │           ▼                 │
   ┌──────────────────────┐                        │     [B, C, D]               │
   │ L2 normalize         │                        └─────────────────────────────┘
   │  → [B, D]            │
   └──────────────────────┘

   Scoring: dot product between user emb & candidate emb → top-K retrieval.
```

A non-MLP mean-pool path exists when `enable_linear_proj=False` (`recsys_retrieval_model.py:74-79`).

## Action heads

### 19 discrete actions (`runners.py:233-253`)

```
 0  favorite_score              10  dwell_score
 1  reply_score                 11  quote_score
 2  repost_score                12  quoted_click_score
 3  photo_expand_score          13  follow_author_score
 4  click_score                 14  not_interested_score
 5  profile_click_score         15  block_author_score
 6  vqv_score                   16  mute_author_score
 7  share_score                 17  report_score
 8  share_via_dm_score          18  dwell_time
 9  share_via_copy_link_score
```

Unembedding matrix: `[config.emb_size, config.num_actions]` (`recsys_model.py:464-474`).

> The demo ranker hard-codes `num_actions=19` (`run_ranker.py:27`); the production pipeline reads `num_actions` from artifact config (`run_pipeline.py:192`). The artifact `oss-phoenix-artifacts.zip` is an **LFS pointer** in this checkout (~2.9 GB uncompressed) — production weights are not bundled.

### 8 continuous predictions (`runners.py:255-264`)

```
 0  reserved
 1  dwell_time         ◀── only one actually used downstream (recsys_model.py:557)
 2  video_watch_time
 3  scroll_depth
 4  reserved_3
 5  reserved_4
 6  reserved_5
 7  reserved_6
```

Continuous head produces sigmoid outputs (`recsys_model.py:674-678`). Only `dwell_time` (index 1) is embedded back as a history feature for ranking:

```python
# recsys_model.py:556-557
if batch.history_continuous_actions is not None:
    dwell_values = batch.history_continuous_actions[:, :, 1]  # index 1 = dwell_time
```

The reserved slots are stubs.

## Hash embeddings

```python
# recsys_model.py:96-100
@dataclass
class HashConfig:
    num_user_hashes:   int = 2
    num_item_hashes:   int = 2
    num_author_hashes: int = 2
    num_ip_hashes:     int = 0   # IP hash channel disabled by default
```

**The 1M-vocab hash tables are NOT in the JAX model.** They live in the data/feature-store layer. The model consumes *pre-looked-up* embeddings of shape `[B, num_hashes, D]` per entity.

The 1M numbers appear in `runners.py:454-456` as example-data defaults (`num_user_embeddings=1_000_000`, etc.) — used to seed `create_example_batch()` for unit tests / demos, not for the production model.

Projection layers (`recsys_model.py:178, 250-262, 300-328`):
- user projection: `[num_user_hashes * D, D]`
- history projection: `[concat_dim, D]`
- candidate projection: `[concat_dim, D]`

## End-to-end pipeline (`run_pipeline.py`)

```
   ┌────────────────────────────────────────────────────┐
   │ Stage 1 — Retrieval (run_pipeline.py:178, 302)     │
   │                                                    │
   │   TOP_K = min(args.top_k_retrieval=200,            │
   │               len(corpus_post_ids))                │
   │                                                    │
   │   user_emb     = user_tower(history)               │
   │   corpus_embs  = candidate_tower(corpus)           │
   │   scores       = user_emb · corpus_embs.T          │
   │   top_K_idx    = top_k(scores, TOP_K)              │
   └────────────────────┬───────────────────────────────┘
                        ▼
   ┌────────────────────────────────────────────────────┐
   │ Stage 2 — Ranking in batches                       │
   │                                                    │
   │   cand_len = ret_cfg["candidate_seq_len"]          │
   │              (run_pipeline.py:194)                 │
   │   ▶ cand_len = 64 in the mini config, BUT it is    │
   │     parameterized — not hardcoded.                 │
   │   ▶ The literal "64" elsewhere (run_pipeline.py    │
   │     :248) is N_neg for retrieval init dummy        │
   │     batches, NOT the ranking batch size.           │
   │                                                    │
   │   for i in range(0, TOP_K, cand_len):              │
   │       batch  = top_K[i : i+cand_len]               │
   │       scores = full_transformer(history + batch)   │
   └────────────────────────────────────────────────────┘
```

## What's external

| Concern | In repo? |
|---------|----------|
| Inference architecture (transformer, towers, masks, heads) | ✅ Present |
| Unit tests | ✅ `test_recsys_model.py`, `test_recsys_retrieval_model.py` |
| Trained checkpoint | ❌ `artifacts/oss-phoenix-artifacts.zip` is an LFS pointer (135-byte pointer file referencing an LFS object of `size 2903518802` bytes — that's the zip object's stored size, not necessarily a measurement of uncompressed weights) |
| Training loops, optimizer state | ❌ Not present |
| Distributed training, gradient checkpointing | ❌ Not present |

## Verification

The codex council reported `uv run pytest test_recsys_model.py test_recsys_retrieval_model.py` returning 34 passing tests. The Phoenix checkout is internally consistent.
