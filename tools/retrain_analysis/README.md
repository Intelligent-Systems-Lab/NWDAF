# Retrain Analysis Report

Generate an offline HTML report from retrain-monitoring experiment outputs.

## Usage

Directory mode:

```bash
uv run retrain_report.py \
  --input /path/to/experiment-dir \
  --out /path/to/experiment-dir/report.html
```

Explicit inputs:

```bash
uv run retrain_report.py \
  --log /path/to/nwdaf.log \
  --config /path/to/nwdafcfg.yaml \
  --out /path/to/report.html
```

Replay trace mode:

```bash
uv run retrain_report.py \
  --input /path/to/replay-trace-dir \
  --out /path/to/replay-trace-dir/report.html
```

Run commands from this directory so `uv` uses `tools/retrain_analysis/pyproject.toml`.

## Inputs

- `nwdaf.log`: policy, lifecycle, raw traffic, and inference logs.
- `nwdafcfg.yaml`: optional threshold/config metadata.
- replay trace directory: optional structured trace output from `tools/retrain_replay`.

The report supports both runtime logs and replay structured traces.

In log mode, it derives its data from these records:

- `Accuracy scope`: metric time series by model and scope.
- `Accuracy policy`: degradation/chronic policy state, signals, and hit counters.
- `latest aggregated slot`: actual UL/DL traffic by subscription and group.
- `ML inference`: predicted UL/DL traffic by subscription and group.
- retrain/training/hot-swap lifecycle messages.

If the input directory contains replay trace artifacts such as
`manifest.json`, `slots.parquet`, `predictions.parquet`, `monitor_rounds.parquet`,
and `policy.parquet`, the report switches to a trace backend instead of parsing
runtime logs.

## Output

A single HTML report with:

- experiment summary
- model/scope alias table
- config threshold table
- lifecycle event table
- metric time-series charts
- actual-vs-predicted UL/DL traffic charts with policy signal markers and retrain lifecycle lines
- policy signal timeline
- decision-window hit charts
- degradation detail charts with signal markers
- chronic traffic-scale detail with signal markers
- retrain trigger explanation table
