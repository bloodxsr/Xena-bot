# Raid ML Sidecar (Rust)

Live Rust scoring service for raid-risk inspection.

## Endpoints

- GET /health
- POST /v1/raid/join
- POST /v1/raid/suspicious

## Run

```bash
cd bot_js/raid_ml_sidecar
cargo run --release
```

Default bind:

- host: 127.0.0.1
- port: 8787

Environment overrides:

- RAID_ML_HOST
- RAID_ML_PORT
- RAID_ML_LEARNING_RATE
- RAID_ML_WEIGHT_DECAY
- RAID_ML_HEURISTIC_BLEND
- RAID_ML_WARMUP_EVENTS
- RAID_ML_BASELINE_ALPHA
- RAID_ML_MAX_WEIGHT_MAGNITUDE
