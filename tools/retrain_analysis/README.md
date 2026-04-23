# Retrain Analysis Report

Generate an offline HTML report from retrain-monitoring experiment outputs.

## Usage

Directory mode:

```bash
uv run retrain_report.py \
  --input ../../.agent/tmp/0422-23 \
  --out ../../.agent/tmp/0422-23/report.html
```

Explicit inputs:

```bash
uv run retrain_report.py \
  --log ../../.agent/tmp/0422-23/nwdaf.log \
  --config ../../.agent/tmp/0422-23/nwdafcfg.yaml \
  --out ../../.agent/tmp/0422-23/report.html
```

Run commands from this directory so `uv` uses `tools/retrain_analysis/pyproject.toml`.

## Inputs

- `nwdaf.log`: policy, lifecycle, raw traffic, and inference logs.
- `nwdafcfg.yaml`: optional threshold/config metadata.

The report is log-only. It derives its data from these log records:

- `Accuracy scope`: metric time series by model and scope.
- `Accuracy policy`: degradation/chronic policy state, signals, and hit counters.
- `latest aggregated slot`: actual UL/DL traffic by subscription and group.
- `ML inference`: predicted UL/DL traffic by subscription and group.
- retrain/training/hot-swap lifecycle messages.

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
