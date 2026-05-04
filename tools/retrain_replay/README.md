# Retrain Replay

Offline replay harness for NWDAF retrain experiments.

It reads `go-upf/pre_data`, runs local inference with the same model bundle
contract used by `NWDAF-ML-Service`, mirrors the current MTLF retrain policy,
optionally calls Daisy `07_MTLF_training`, and writes structured trace output.

## Usage

```bash
uv run retrain_replay.py run \
  --dataset-root /path/to/pre_data \
  --config /path/to/nwdafcfg.yaml \
  --replay-config ./replay_config.yaml \
  --initial-bundle /path/to/initial-bundle \
  --daisy-example-dir /path/to/daisy/examples/07_MTLF_training \
  --out /path/to/output/replay-test
```

The output directory is designed to be consumed by `tools/retrain_analysis`:

```bash
cd ../retrain_analysis
uv run retrain_report.py --input /path/to/output/replay-test --out /path/to/output/replay-test/report.html
```

## Local Initial Training

To train an initial bundle without Daisy/FL, use the local supervised trainer:

```bash
uv run train_initial_local.py \
  --dataset-root /path/to/pre_data \
  --config /path/to/nwdafcfg.yaml \
  --groups group1 group2 \
  --cat-duration 1800 \
  --period 30 \
  --seq-length 30 \
  --out /path/to/output/initial_local_cat1_30s
```

The generated bundle is replay-compatible:

```bash
uv run retrain_replay.py run \
  --dataset-root /path/to/pre_data \
  --config /path/to/nwdafcfg.yaml \
  --replay-config ./replay_config.yaml \
  --initial-bundle /path/to/output/initial_local_cat1_30s/bundle \
  --daisy-example-dir /path/to/daisy/examples/07_MTLF_training \
  --out /path/to/output/replay-with-local-initial
```

The trainer writes:

- `bundle/` with `config.json`, `model.npy`, `model.py`, `scaler.pkl`
- `meta.json` with run configuration and sample counts
- `train_metrics.json` with train/validation loss history
- `val_predictions.parquet` with held-out next-slot predictions

## Notes

- The loader filters invalid `ts < 0` packet rows before slot aggregation.
  The current `pre_data` contains one such sentinel row per group; keeping it
  would corrupt the first slot and pull the rendered timeline back into `1920`.
- By default the replay mirrors `go-upf` pseudo-driver warm start.
  It reads each group's `file.json`, uses `breaking time` as the Phase 1/2
  split, preloads historical slots before that boundary into the model history,
  and only starts emitting predictions from the aligned live boundary.
  This behavior is controlled by `dataset.use_pseudo_warmstart` and the
  optional `dataset.breaking_time_sec` override in `replay_config.yaml`.
- The replay mirrors NWDAF's current `minSamples` gate at the monitor-round
  level: policy evaluation is enabled only when the total number of mature
  matched pairs in that round meets the threshold. Per-scope `sampleCount`
  is still recorded separately in `monitor_rounds.parquet`.
- `--skip-daisy` still emits a mock retrain lifecycle using
  `retrain.mock_training_duration_sec` from `replay_config.yaml`, so policy
  evaluation does not immediately re-trigger forever.
- During retraining and pending swap, monitor rounds continue to be recorded,
  but policy evaluation is intentionally suppressed until the new model becomes
  effective on the simulated timeline.
- Daisy can use a separate interpreter via `daisy.python_bin`. This is
  intended for setups where Daisy 07 still needs Python `3.8` while the replay
  and report tools run on a newer Python.
- Real Daisy mode uses the async callback path, because Daisy only packages
  downloadable artifacts on the callback workflow.
