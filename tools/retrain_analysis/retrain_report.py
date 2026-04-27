#!/usr/bin/env python3
"""
Generate an offline HTML report from NWDAF retrain-monitoring experiment output.

The report consumes NWDAF logs and optional config YAML. It is analysis tooling
only and does not participate in runtime retrain decisions.
"""

from __future__ import annotations

import argparse
import html
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
import yaml
from jinja2 import Template
from plotly.subplots import make_subplots


LOG_TIME_RE = re.compile(r'time="(?P<time>[^"]+)"')
LOG_MSG_RE = re.compile(r'msg="(?P<msg>(?:\\.|[^"])*)"')
POLICY_RE = re.compile(r"Accuracy policy \[(?P<model>.*?)\]: (?P<body>.*)")
RETRAIN_RE = re.compile(r"Retrain trigger \[(?P<model>.*?)\]: (?P<body>.*)")
ACCURACY_SCOPE_RE = re.compile(r"Accuracy scope \[(?P<model>.*?)\]: (?P<body>.*)")
AGGREGATED_SLOT_RE = re.compile(r"latest aggregated slot: (?P<sub>\S+) (?P<body>.*)")
ML_INFERENCE_RE = re.compile(r"ML inference: sub=(?P<sub>\S+) (?P<body>.*)")
KV_RE = re.compile(r"(?P<key>[A-Za-z][A-Za-z0-9_]*)=(?P<value>\S+)")
HITS_RE = re.compile(r"^(?P<count>\d+)/(?P<required>\d+)$")
METRIC_ORDER = ["MAE", "WAPE", "NRMSE", "sMAPE", "MSE"]


@dataclass
class Inputs:
    log: Path | None
    config: Path | None
    out: Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a NWDAF retrain monitoring HTML analysis report.",
    )
    parser.add_argument("--input", type=Path, help="Experiment directory.")
    parser.add_argument("--log", type=Path, help="Path to nwdaf.log.")
    parser.add_argument("--config", type=Path, help="Path to nwdafcfg.yaml.")
    parser.add_argument("--out", type=Path, required=True, help="Output HTML path.")
    return parser.parse_args()


def discover_inputs(args: argparse.Namespace) -> Inputs:
    input_dir = args.input
    log_path = args.log
    config_path = args.config

    if input_dir:
        if not log_path:
            candidate = input_dir / "nwdaf.log"
            if candidate.exists():
                log_path = candidate
        if not config_path:
            candidate = input_dir / "nwdafcfg.yaml"
            if candidate.exists():
                config_path = candidate

    return Inputs(
        log=log_path,
        config=config_path,
        out=args.out,
    )


def read_accuracy_config(path: Path | None) -> dict[str, Any]:
    if not path or not path.exists():
        return {}
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    cfg = (
        data.get("configuration", {})
        .get("mtlf", {})
        .get("accuracyMonitor", {})
    )
    if not isinstance(cfg, dict):
        return {}
    chronic = cfg.get("chronicPolicy")
    if not isinstance(chronic, dict):
        cfg["chronicPolicy"] = {}
    return cfg


def parse_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text == "true":
        return True
    if text == "false":
        return False
    return None


def parse_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def parse_seconds(value: Any) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if text.endswith("s"):
        text = text[:-1]
    return parse_int(text)


def parse_hit_pair(value: str | None) -> tuple[int | None, int | None]:
    if not value:
        return None, None
    match = HITS_RE.match(value)
    if not match:
        return None, None
    return int(match.group("count")), int(match.group("required"))


def parse_timestamp(value: str | None) -> pd.Timestamp | pd.NaT:
    if not value:
        return pd.NaT
    text = value.strip()
    if "." in text and text.endswith("Z"):
        prefix, suffix = text[:-1].split(".", 1)
        text = f"{prefix}.{suffix[:6]}Z"
    try:
        return pd.to_datetime(text, utc=True)
    except Exception:
        return pd.NaT


def extract_log_time(line: str) -> pd.Timestamp | pd.NaT:
    match = LOG_TIME_RE.search(line)
    return parse_timestamp(match.group("time")) if match else pd.NaT


def extract_log_msg(line: str) -> str | None:
    match = LOG_MSG_RE.search(line)
    if not match:
        return None
    return bytes(match.group("msg"), "utf-8").decode("unicode_escape")


def parse_kv(body: str) -> dict[str, str]:
    return {m.group("key"): m.group("value") for m in KV_RE.finditer(body)}


def scope_from_kv(kv: dict[str, str]) -> str | None:
    if group := kv.get("group"):
        return f"group:{group}"
    if supi := kv.get("supi"):
        return f"supi:{supi}"
    return kv.get("scope")


def floor_timestamp(timestamp: pd.Timestamp, seconds: int) -> pd.Timestamp | pd.NaT:
    if pd.isna(timestamp) or seconds <= 0:
        return pd.NaT
    unix = int(timestamp.timestamp())
    return pd.to_datetime((unix // seconds) * seconds, unit="s", utc=True)


def model_label(model_url: str | None, index: int | None = None) -> str:
    if not model_url:
        return "unknown"
    if model_url.endswith("/initial") or model_url == "http://placeholder/download/initial":
        return "initial"
    tail = model_url.rstrip("/").split("/")[-1]
    if tail:
        return tail[:12]
    if index is not None:
        return f"model-{index}"
    return "model"


def scope_label(scope: str | None, index: int | None = None) -> str:
    if not scope:
        return "unknown"
    short = scope
    for prefix in ("group:", "supi:"):
        if short.startswith(prefix):
            short = short[len(prefix):]
            break
    if index is None:
        return short
    return f"S{index} {short}"


def add_model_labels(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "model" not in df.columns:
        return df
    models = {model: model_label(model, idx) for idx, model in enumerate(df["model"].dropna().unique(), start=1)}
    df = df.copy()
    df["modelLabel"] = df["model"].map(models).fillna("unknown")
    return df


def first_seen(df: pd.DataFrame, column: str, time_column: str) -> pd.Series:
    if df.empty or column not in df.columns:
        return pd.Series(dtype="datetime64[ns, UTC]")
    if time_column not in df.columns:
        return df.dropna(subset=[column]).groupby(column).size().index.to_series().map(lambda _: pd.NaT)
    return df.dropna(subset=[column]).groupby(column)[time_column].min()


def build_alias_maps(frames: list[pd.DataFrame]) -> tuple[dict[str, str], dict[str, str]]:
    model_seen = []
    scope_seen = []
    for df in frames:
        if df.empty:
            continue
        time_col = "timestamp" if "timestamp" in df.columns else "targetTime"
        if "model" in df.columns:
            model_seen.append(first_seen(df, "model", time_col))
        if "scope" in df.columns:
            scope_seen.append(first_seen(df, "scope", time_col))

    model_map: dict[str, str] = {}
    if model_seen:
        models = pd.concat(model_seen).groupby(level=0).min().sort_values(kind="stable")
        for idx, model in enumerate(models.index, start=1):
            model_map[str(model)] = f"M{idx} {model_label(str(model))}"

    scope_map: dict[str, str] = {}
    if scope_seen:
        scopes = pd.concat(scope_seen).groupby(level=0).min().sort_values(kind="stable")
        for idx, scope in enumerate(scopes.index, start=1):
            scope_map[str(scope)] = scope_label(str(scope), idx)

    return model_map, scope_map


def apply_aliases(df: pd.DataFrame, model_map: dict[str, str], scope_map: dict[str, str]) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    if "model" in df.columns:
        df["modelLabel"] = df["model"].map(model_map).fillna(df.get("modelLabel", "unknown"))
    if "scope" in df.columns:
        df["scopeLabel"] = df["scope"].map(scope_map).fillna(df["scope"])
    return df


def alias_table(model_map: dict[str, str], scope_map: dict[str, str]) -> str:
    model_rows = [{"kind": "model", "label": label, "fullValue": value} for value, label in model_map.items()]
    scope_rows = [{"kind": "scope", "label": label, "fullValue": value} for value, label in scope_map.items()]
    rows = model_rows + scope_rows
    if not rows:
        return "<p>No model/scope aliases available.</p>"
    return pd.DataFrame(rows).to_html(index=False, classes="data-table", escape=True)


def parse_metric_values(value: str | None) -> dict[str, float | None]:
    if not value:
        return {}
    metrics: dict[str, float | None] = {}
    for item in value.split(","):
        if "=" not in item:
            continue
        key, raw = item.split("=", 1)
        metrics[key] = parse_float(raw)
    return metrics


def parse_accuracy_scope_rows(timestamp: pd.Timestamp, model: str, body: str) -> list[dict[str, Any]]:
    kv = parse_kv(body)
    rows = []
    for metric, current in parse_metric_values(kv.get("metrics")).items():
        rows.append(
            {
                "timestamp": timestamp,
                "model": model,
                "scope": kv.get("scope"),
                "metric": metric,
                "current": current,
                "sampleCount": parse_int(kv.get("samples")),
                "inferenceNum": None,
                "windowStart": pd.NaT,
                "windowEnd": pd.NaT,
            }
        )
    return rows


def parse_policy_row(timestamp: pd.Timestamp, model: str, body: str) -> dict[str, Any]:
    kv = parse_kv(body)
    degradation_signal_raw = kv.get("degradationSignal", kv.get("relGate"))
    hit_reason = kv.get("hitReason", kv.get("triggerReason", "none"))
    degradation_hits, degradation_required = parse_hit_pair(kv.get("degradationHits"))
    chronic_hits, chronic_required = parse_hit_pair(kv.get("chronicHits"))

    chronic_signal_raw = kv.get("chronicSignal")
    if chronic_signal_raw is None:
        chronic_signal_raw = "true" if hit_reason in {"chronic", "both"} else "false"

    return {
        "timestamp": timestamp,
        "model": model,
        "scope": kv.get("scope"),
        "metric": kv.get("metric"),
        "current": parse_float(kv.get("current")),
        "mean": parse_float(kv.get("mean")),
        "std": parse_float(kv.get("std")),
        "zscore": parse_float(kv.get("zscore")),
        "degradationEligible": parse_bool(kv.get("degradationEligible", kv.get("absGate"))),
        "degradationSignal": degradation_signal_raw,
        "baselineReady": parse_bool(kv.get("baselineReady")),
        "trafficScale": parse_float(kv.get("trafficScale")),
        "chronicEligible": parse_bool(kv.get("chronicEligible")),
        "chronicSignal": chronic_signal_raw,
        "chronicValue": parse_float(kv.get("chronicValue")),
        "degradationHits": degradation_hits,
        "degradationRequired": degradation_required,
        "chronicHits": chronic_hits,
        "chronicRequired": chronic_required,
        "hitReason": hit_reason,
    }


def event_row(timestamp: pd.Timestamp, event_type: str, detail: str, **values: Any) -> dict[str, Any]:
    row = {
        "timestamp": timestamp,
        "eventType": event_type,
        "model": values.pop("model", None),
        "scope": values.pop("scope", None),
        "reason": values.pop("reason", None),
        "detail": detail,
    }
    row.update(values)
    return row


def parse_event(timestamp: pd.Timestamp, msg: str) -> dict[str, Any] | None:
    if match := RETRAIN_RE.match(msg):
        kv = parse_kv(match.group("body"))
        return event_row(
            timestamp,
            "retrain_trigger",
            msg,
            model=match.group("model"),
            scope=kv.get("scope"),
            reason=kv.get("reason"),
            current=parse_float(kv.get("current")),
            degradationHits=kv.get("degradationHits"),
            chronicHits=kv.get("chronicHits"),
        )
    if "Async training request accepted" in msg:
        kv = parse_kv(msg)
        return event_row(timestamp, "training_accepted", msg, taskId=kv.get("taskId"))
    if "Async training complete" in msg:
        kv = parse_kv(msg)
        return event_row(timestamp, "training_complete", msg, model=kv.get("modelUrl"), taskId=kv.get("taskId"))
    if "Async training failed" in msg:
        kv = parse_kv(msg)
        return event_row(timestamp, "training_failed", msg, taskId=kv.get("taskId"))
    if "Starting model hot-swap" in msg:
        kv = parse_kv(msg)
        return event_row(timestamp, "hot_swap_start", msg, model=kv.get("old"), newModel=kv.get("new"))
    if "Model hot-swap completed successfully" in msg:
        kv = parse_kv(msg)
        return event_row(timestamp, "hot_swap_complete", msg, modelId=kv.get("modelId"))
    if "Accuracy monitor warmup:" in msg:
        kv = parse_kv(msg.replace(",", ""))
        return event_row(timestamp, "warmup_start", msg, model=kv.get("model"))
    if "Accuracy monitor warmup complete:" in msg:
        kv = parse_kv(msg)
        return event_row(timestamp, "warmup_complete", msg, model=kv.get("model"))
    if "Accuracy monitor warmup skipped:" in msg:
        kv = parse_kv(msg)
        return event_row(timestamp, "warmup_skipped", msg, model=kv.get("model"))
    if "Not enough samples" in msg:
        match = re.search(r"Not enough samples \[(?P<model>.*?)\]: (?P<got>\d+) < (?P<want>\d+)", msg)
        return event_row(
            timestamp,
            "not_enough_samples",
            msg,
            model=match.group("model") if match else None,
            got=parse_int(match.group("got")) if match else None,
            want=parse_int(match.group("want")) if match else None,
        )
    return None


def parse_traffic_row(timestamp: pd.Timestamp, msg: str) -> dict[str, Any] | None:
    if match := AGGREGATED_SLOT_RE.match(msg):
        kv = parse_kv(match.group("body"))
        return {
            "timestamp": parse_timestamp(kv.get("ts")),
            "logTimestamp": timestamp,
            "source": "actual",
            "nwdafSubId": match.group("sub"),
            "scope": scope_from_kv(kv),
            "ulVol": parse_float(kv.get("ulVol")),
            "dlVol": parse_float(kv.get("dlVol")),
            "steps": None,
            "confidence": None,
        }

    if match := ML_INFERENCE_RE.match(msg):
        kv = parse_kv(match.group("body"))
        steps = parse_int(kv.get("steps"))
        comm_dur = parse_seconds(kv.get("commDur"))
        if not steps or not comm_dur or steps != 1:
            return None
        interval = comm_dur // steps
        target_time = floor_timestamp(timestamp, interval)
        if not pd.isna(target_time):
            target_time = target_time + pd.Timedelta(seconds=interval)
        return {
            "timestamp": target_time,
            "logTimestamp": timestamp,
            "source": "predicted",
            "nwdafSubId": match.group("sub"),
            "scope": scope_from_kv(kv),
            "ulVol": parse_float(kv.get("ulVol")),
            "dlVol": parse_float(kv.get("dlVol")),
            "steps": steps,
            "confidence": parse_float(kv.get("confidence")),
        }

    return None


def parse_log(path: Path | None) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    metric_rows: list[dict[str, Any]] = []
    policy_rows: list[dict[str, Any]] = []
    event_rows: list[dict[str, Any]] = []
    traffic_rows: list[dict[str, Any]] = []
    if not path or not path.exists():
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        timestamp = extract_log_time(line)
        msg = extract_log_msg(line)
        if not msg:
            continue
        traffic = parse_traffic_row(timestamp, msg)
        if traffic:
            traffic_rows.append(traffic)
        if match := ACCURACY_SCOPE_RE.match(msg):
            metric_rows.extend(parse_accuracy_scope_rows(timestamp, match.group("model"), match.group("body")))
            continue
        if match := POLICY_RE.match(msg):
            policy_rows.append(parse_policy_row(timestamp, match.group("model"), match.group("body")))
            continue
        event = parse_event(timestamp, msg)
        if event:
            event_rows.append(event)

    metrics_df = pd.DataFrame(metric_rows)
    policy_df = pd.DataFrame(policy_rows)
    events_df = pd.DataFrame(event_rows)
    traffic_df = pd.DataFrame(traffic_rows)
    if not metrics_df.empty:
        metrics_df = add_model_labels(metrics_df).sort_values("timestamp")
    if not policy_df.empty:
        policy_df = add_model_labels(policy_df).sort_values("timestamp")
    if not events_df.empty:
        events_df = add_model_labels(events_df).sort_values("timestamp")
    if not traffic_df.empty:
        traffic_df = traffic_df.sort_values("timestamp")
    return metrics_df, policy_df, events_df, traffic_df


def config_value(cfg: dict[str, Any], path: str, default: Any = None) -> Any:
    cur: Any = cfg
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur


def make_summary(
    metrics_df: pd.DataFrame,
    policy_df: pd.DataFrame,
    events_df: pd.DataFrame,
    traffic_df: pd.DataFrame,
    inputs: Inputs,
) -> dict[str, Any]:
    event_counts = events_df["eventType"].value_counts().to_dict() if not events_df.empty else {}
    timestamps = []
    for df in [metrics_df, policy_df, events_df, traffic_df]:
        if not df.empty and "timestamp" in df.columns:
            timestamps.extend(df["timestamp"].dropna().tolist())
    return {
        "inputLog": str(inputs.log) if inputs.log else "-",
        "config": str(inputs.config) if inputs.config else "-",
        "timeStart": min(timestamps).isoformat() if timestamps else "-",
        "timeEnd": max(timestamps).isoformat() if timestamps else "-",
        "models": int(metrics_df["model"].nunique()) if not metrics_df.empty else 0,
        "scopes": int(metrics_df["scope"].nunique()) if not metrics_df.empty else 0,
        "metricsRows": len(metrics_df),
        "policyRows": len(policy_df),
        "trafficRows": len(traffic_df),
        "events": event_counts,
    }


def empty_figure(title: str) -> go.Figure:
    fig = go.Figure()
    fig.update_layout(title=title, annotations=[{"text": "No data", "xref": "paper", "yref": "paper", "showarrow": False}])
    return fig


def natural_sort_text(value: Any) -> str:
    return "".join(part.zfill(8) if part.isdigit() else part.lower() for part in re.split(r"(\d+)", str(value)))


def ordered_values(df: pd.DataFrame, column: str) -> list[str]:
    if column not in df.columns:
        return []
    values = [str(value) for value in df[column].dropna().unique()]
    if column == "metric":
        known = [metric for metric in METRIC_ORDER if metric in values]
        unknown = sorted([metric for metric in values if metric not in METRIC_ORDER], key=natural_sort_text)
        return known + unknown
    return sorted(values, key=natural_sort_text)


def sort_for_chart(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    existing = [column for column in columns if column in df.columns]
    if not existing:
        return df
    return df.sort_values(existing, key=lambda series: series.map(natural_sort_text), kind="stable")


def category_orders(df: pd.DataFrame, columns: list[str]) -> dict[str, list[str]]:
    return {column: ordered_values(df, column) for column in columns if column in df.columns}


def add_model_scope_label(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    model_col = "modelLabel" if "modelLabel" in df.columns else "model"
    scope_col = "scopeLabel" if "scopeLabel" in df.columns else "scope"
    df["modelScopeLabel"] = df[model_col].astype(str) + " | " + df[scope_col].astype(str)
    return df


def metric_chart(metrics_df: pd.DataFrame, title: str, metric: str | None = None, threshold: float | None = None) -> go.Figure:
    if metrics_df.empty:
        return empty_figure(title)
    df = metrics_df.copy()
    if metric:
        df = df[df["metric"] == metric]
    if df.empty:
        return empty_figure(title)
    df = add_model_scope_label(df)
    df = sort_for_chart(df, ["metric", "modelLabel", "scopeLabel", "timestamp"])
    fig = px.line(
        df,
        x="timestamp",
        y="current",
        color="modelScopeLabel",
        facet_row=None if metric else "metric",
        hover_data=["model", "scope", "metric", "sampleCount", "inferenceNum", "windowStart", "windowEnd"],
        category_orders={
            **category_orders(df, ["modelScopeLabel"]),
            "metric": ordered_values(df, "metric"),
        },
        title=title,
    )
    if threshold is not None:
        fig.add_hline(y=threshold, line_dash="dash", annotation_text="threshold")
    if not metric:
        fig.update_yaxes(matches=None, showticklabels=True)
    fig.update_layout(
        height=420 if metric else max(520, 220 * int(df["metric"].nunique())),
        legend_title_text="Model alias | scope alias",
    )
    return fig


def metric_series_charts(metrics_df: pd.DataFrame, primary_metric: str, fixed_floor: float | None) -> list[go.Figure]:
    if metrics_df.empty:
        return [empty_figure("Metric Time Series")]
    metrics = ordered_values(metrics_df, "metric")
    return [
        metric_chart(
            metrics_df,
            f"Metric Time Series: {metric}",
            metric,
            fixed_floor if metric == primary_metric else None,
        )
        for metric in metrics
    ]


def policy_signal_chart(policy_df: pd.DataFrame) -> go.Figure:
    if policy_df.empty:
        return empty_figure("Policy State Timeline")
    policy_df = sort_for_chart(policy_df, ["modelLabel", "scopeLabel", "timestamp"])
    states = [
        "baselineReady",
        "degradationEligible",
        "degradationSignal",
        "chronicEligible",
        "chronicSignal",
    ]
    rows = []
    for _, row in policy_df.iterrows():
        prefix = f"{row.get('modelLabel', 'model')} | {row.get('scopeLabel', row.get('scope', '-'))}"
        for state in states:
            value = row.get(state)
            if pd.isna(value):
                label = "missing"
                color = "#9ca3af"
            else:
                label = str(value)
                if label.lower() == "true":
                    color = "#16a34a"
                elif label.lower() == "false":
                    color = "#dc2626"
                elif label.lower() == "skipped":
                    color = "#9ca3af"
                else:
                    color = "#64748b"
            rows.append(
                {
                    "timestamp": row["timestamp"],
                    "lane": f"{prefix} | {state}",
                    "value": label,
                    "color": color,
                    "hitReason": row.get("hitReason"),
                }
            )
    df = pd.DataFrame(rows)
    lane_order = list(reversed(ordered_values(df, "lane")))
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=df["timestamp"],
            y=df["lane"],
            mode="markers",
            marker={"size": 9, "color": df["color"]},
            customdata=df[["value", "hitReason"]],
            hovertemplate="time=%{x}<br>%{y}<br>value=%{customdata[0]}<br>hitReason=%{customdata[1]}<extra></extra>",
        )
    )
    fig.update_layout(title="Policy State Timeline", height=max(520, 18 * df["lane"].nunique()))
    fig.update_yaxes(categoryorder="array", categoryarray=lane_order)
    return fig


def hits_chart(policy_df: pd.DataFrame) -> go.Figure:
    if policy_df.empty:
        return empty_figure("Decision Window Hits")
    rows = []
    for _, row in policy_df.iterrows():
        label = f"{row.get('modelLabel', 'model')} | {row.get('scopeLabel', row.get('scope', '-'))}"
        rows.append({"timestamp": row["timestamp"], "scope": label, "path": "degradation", "hits": row.get("degradationHits")})
        rows.append({"timestamp": row["timestamp"], "scope": label, "path": "chronic", "hits": row.get("chronicHits")})
    df = pd.DataFrame(rows).dropna(subset=["hits"])
    if df.empty:
        return empty_figure("Decision Window Hits")
    df = sort_for_chart(df, ["scope", "path", "timestamp"])
    return px.line(
        df,
        x="timestamp",
        y="hits",
        color="scope",
        line_dash="path",
        category_orders=category_orders(df, ["scope", "path"]),
        title="Decision Window Hits",
    )


def degradation_detail_chart(policy_df: pd.DataFrame, cfg: dict[str, Any]) -> go.Figure:
    if policy_df.empty:
        return empty_figure("Degradation Detail")
    df = policy_df.dropna(subset=["current", "mean", "std", "zscore"], how="all")
    if df.empty:
        return empty_figure("Degradation Detail")

    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.08,
        subplot_titles=("Metric scale: current / mean / std", "Z-score signal"),
    )

    scope_col = "scopeLabel" if "scopeLabel" in df.columns else "scope"
    df = sort_for_chart(df, ["modelLabel", scope_col, "timestamp"])
    for label, group in df.groupby(["modelLabel", scope_col], dropna=False, sort=False):
        name = " | ".join(str(part) for part in label)
        fig.add_trace(go.Scatter(x=group["timestamp"], y=group["current"], name=f"{name} current"), row=1, col=1)
        fig.add_trace(go.Scatter(x=group["timestamp"], y=group["mean"], name=f"{name} mean", line={"dash": "dash"}), row=1, col=1)
        fig.add_trace(go.Scatter(x=group["timestamp"], y=group["std"], name=f"{name} std", line={"dash": "dot"}), row=1, col=1)
        fig.add_trace(go.Scatter(x=group["timestamp"], y=group["zscore"], name=f"{name} zscore"), row=2, col=1)

    signals = df[df["degradationSignal"].astype(str).str.lower() == "true"].dropna(subset=["zscore"])
    if not signals.empty:
        fig.add_trace(
            go.Scatter(
                x=signals["timestamp"],
                y=signals["zscore"],
                mode="markers",
                name="degradation signal",
                marker={"size": 10, "symbol": "triangle-up", "color": "#dc2626"},
                customdata=signals[[scope_col, "hitReason", "current", "degradationHits"]],
                hovertemplate=(
                    "time=%{x}<br>degradation signal<br>scope=%{customdata[0]}"
                    "<br>hitReason=%{customdata[1]}<br>current=%{customdata[2]}"
                    "<br>degradationHits=%{customdata[3]}<extra></extra>"
                ),
            ),
            row=2,
            col=1,
        )

    fixed_floor = parse_float(config_value(cfg, "fixedFloor"))
    z_threshold = parse_float(config_value(cfg, "zScoreThreshold"))
    if fixed_floor is not None:
        fig.add_hline(y=fixed_floor, line_dash="dash", annotation_text="fixedFloor", row=1, col=1)
    if z_threshold is not None:
        fig.add_hline(y=z_threshold, line_dash="dash", annotation_text="zScoreThreshold", row=2, col=1)

    fig.update_yaxes(title_text="metric value", row=1, col=1)
    fig.update_yaxes(title_text="zscore", row=2, col=1)
    fig.update_layout(title="Degradation Detail", height=720, legend_traceorder="grouped")
    return fig


def chronic_traffic_scale_chart(policy_df: pd.DataFrame, cfg: dict[str, Any]) -> go.Figure:
    if policy_df.empty:
        return empty_figure("Chronic Traffic Scale")
    df = policy_df.dropna(subset=["trafficScale"])
    if df.empty:
        return empty_figure("Chronic Traffic Scale")
    fig = go.Figure()
    scope_col = "scopeLabel" if "scopeLabel" in df.columns else "scope"
    df = sort_for_chart(df, ["modelLabel", scope_col, "timestamp"])
    for label, group in df.groupby(["modelLabel", scope_col], dropna=False, sort=False):
        name = " | ".join(str(part) for part in label)
        fig.add_trace(go.Scatter(x=group["timestamp"], y=group["trafficScale"], name=name))
    min_scale = parse_float(config_value(cfg, "chronicPolicy.minTrafficScale"))
    if min_scale is not None:
        fig.add_hline(y=min_scale, line_dash="dash", annotation_text="minTrafficScale")
    fig.update_layout(title="Chronic Traffic Scale", height=420)
    fig.update_yaxes(title_text="trafficScale")
    return fig


def chronic_value_chart(policy_df: pd.DataFrame, cfg: dict[str, Any]) -> go.Figure:
    if policy_df.empty:
        return empty_figure("Chronic Value")
    df = policy_df.dropna(subset=["chronicValue"])
    if df.empty:
        return empty_figure("Chronic Value")
    fig = go.Figure()
    scope_col = "scopeLabel" if "scopeLabel" in df.columns else "scope"
    df = sort_for_chart(df, ["modelLabel", scope_col, "timestamp"])
    for label, group in df.groupby(["modelLabel", scope_col], dropna=False, sort=False):
        name = " | ".join(str(part) for part in label)
        fig.add_trace(go.Scatter(x=group["timestamp"], y=group["chronicValue"], name=name))
    signals = df[df["chronicSignal"].map(lambda value: str(value).lower() == "true")].dropna(subset=["chronicValue"])
    if not signals.empty:
        fig.add_trace(
            go.Scatter(
                x=signals["timestamp"],
                y=signals["chronicValue"],
                mode="markers",
                name="chronic signal",
                marker={"size": 10, "symbol": "diamond", "color": "#f97316"},
                customdata=signals[[scope_col, "hitReason", "chronicValue", "chronicHits"]],
                hovertemplate=(
                    "time=%{x}<br>chronic signal<br>scope=%{customdata[0]}"
                    "<br>hitReason=%{customdata[1]}<br>chronicValue=%{customdata[2]}"
                    "<br>chronicHits=%{customdata[3]}<extra></extra>"
                ),
            ),
        )
    chronic_threshold = parse_float(config_value(cfg, "chronicPolicy.threshold"))
    if chronic_threshold is not None:
        fig.add_hline(y=chronic_threshold, line_dash="dot", annotation_text="chronicThreshold")
    fig.update_layout(title="Chronic Value", height=420)
    fig.update_yaxes(title_text="chronicValue")
    return fig


def add_policy_signal_markers(fig: go.Figure, policy_df: pd.DataFrame, y_value: float) -> None:
    if policy_df.empty or y_value <= 0:
        return
    rows = []
    for _, row in policy_df.iterrows():
        degradation_signal = str(row.get("degradationSignal")).lower() == "true"
        chronic_signal = str(row.get("chronicSignal")).lower() == "true"
        label = row.get("scopeLabel", row.get("scope", "-"))
        if degradation_signal:
            rows.append({**row.to_dict(), "signal": "degradation", "label": label})
        if chronic_signal:
            rows.append({**row.to_dict(), "signal": "chronic", "label": label})
    if not rows:
        return
    signals = pd.DataFrame(rows)
    symbols = {"degradation": "triangle-up", "chronic": "diamond"}
    colors = {"degradation": "#dc2626", "chronic": "#f97316"}
    for signal, group in signals.groupby("signal", sort=False):
        fig.add_trace(
            go.Scatter(
                x=group["timestamp"],
                y=[y_value] * len(group),
                mode="markers",
                name=f"{signal} signal",
                marker={"size": 10, "symbol": symbols.get(signal, "circle"), "color": colors.get(signal, "#64748b")},
                customdata=group[["label", "hitReason", "current", "degradationHits", "chronicHits"]],
                hovertemplate=(
                    "time=%{x}<br>signal="
                    + signal
                    + "<br>scope=%{customdata[0]}<br>hitReason=%{customdata[1]}"
                    + "<br>current=%{customdata[2]}<br>degradationHits=%{customdata[3]}"
                    + "<br>chronicHits=%{customdata[4]}<extra></extra>"
                ),
            )
        )


def add_lifecycle_markers(fig: go.Figure, events_df: pd.DataFrame, y_value: float) -> None:
    if events_df.empty or y_value <= 0:
        return
    styles = {
        "retrain_trigger": ("#dc2626", "dot", "triangle-up", "trigger"),
        "training_accepted": ("#2563eb", "dash", "circle", "train start"),
        "training_complete": ("#16a34a", "dash", "square", "train done"),
        "hot_swap_complete": ("#0f766e", "solid", "diamond", "swap done"),
    }
    lifecycle = events_df[events_df["eventType"].isin(styles.keys())]
    for event_type, group in lifecycle.groupby("eventType", sort=False):
        color, dash, symbol, label = styles[str(event_type)]
        for timestamp in group["timestamp"]:
            if hasattr(timestamp, "to_pydatetime"):
                timestamp = timestamp.to_pydatetime()
            fig.add_shape(
                type="line",
                x0=timestamp,
                x1=timestamp,
                y0=0,
                y1=1,
                xref="x",
                yref="paper",
                line={"color": color, "dash": dash, "width": 1},
            )
        fig.add_trace(
            go.Scatter(
                x=group["timestamp"],
                y=[y_value] * len(group),
                mode="markers",
                name=label,
                marker={"size": 9, "symbol": symbol, "color": color},
                customdata=group[["eventType", "reason", "detail"]],
                hovertemplate="time=%{x}<br>event=%{customdata[0]}<br>reason=%{customdata[1]}<br>%{customdata[2]}<extra></extra>",
            )
        )


def traffic_timeline_chart(
    traffic_df: pd.DataFrame,
    policy_df: pd.DataFrame,
    events_df: pd.DataFrame,
    direction: str,
) -> go.Figure:
    if traffic_df.empty:
        return empty_figure("Actual vs Predicted Traffic")
    value_col = "ulVol" if direction == "UL" else "dlVol"
    required = {"timestamp", "source", "scope", value_col}
    if not required.issubset(set(traffic_df.columns)):
        return empty_figure("Actual vs Predicted Traffic")

    long_df = traffic_df.copy()
    if "scopeLabel" not in long_df.columns:
        long_df["scopeLabel"] = long_df["scope"]
    long_df["volume"] = pd.to_numeric(long_df[value_col], errors="coerce")
    long_df = long_df.dropna(subset=["timestamp", "volume"])
    if long_df.empty:
        return empty_figure(f"Actual vs Predicted Traffic ({direction})")
    long_df["lineLabel"] = long_df["scopeLabel"].astype(str) + " | " + long_df["source"].astype(str)
    long_df = sort_for_chart(long_df, ["scopeLabel", "source", "timestamp"])

    fig = go.Figure()
    for (scope, source), group in long_df.groupby(["scopeLabel", "source"], sort=False):
        fig.add_trace(
            go.Scatter(
                x=group["timestamp"],
                y=group["volume"],
                mode="lines+markers",
                name=f"{scope} | {source}",
                line={"dash": "dash" if source == "predicted" else "solid"},
                customdata=group[["scope", "nwdafSubId", "logTimestamp"]],
                hovertemplate=(
                    "time=%{x}<br>volume=%{y}<br>scope=%{customdata[0]}"
                    "<br>sub=%{customdata[1]}<br>logTime=%{customdata[2]}<extra></extra>"
                ),
            )
        )
    max_volume = float(long_df["volume"].max()) if not long_df.empty else 0
    add_policy_signal_markers(fig, policy_df, max_volume * 1.08)
    add_lifecycle_markers(fig, events_df, max_volume * 1.16)
    fig.update_yaxes(title_text="volume")
    fig.update_layout(
        title=f"Actual vs Predicted Traffic ({direction})",
        height=520,
        legend_title_text="Scope alias | source",
    )
    return fig


def event_table(events_df: pd.DataFrame) -> str:
    if events_df.empty:
        return "<p>No lifecycle events parsed.</p>"
    cols = [col for col in ["timestamp", "eventType", "modelLabel", "scopeLabel", "reason", "detail"] if col in events_df.columns]
    return events_df[cols].to_html(index=False, classes="data-table", escape=True)


def trigger_table(events_df: pd.DataFrame, policy_df: pd.DataFrame) -> str:
    if events_df.empty:
        return "<p>No retrain triggers parsed.</p>"
    triggers = events_df[events_df["eventType"] == "retrain_trigger"].copy()
    if triggers.empty:
        return "<p>No retrain triggers parsed.</p>"
    cols = [col for col in ["timestamp", "modelLabel", "scopeLabel", "reason", "current", "degradationHits", "chronicHits", "detail"] if col in triggers.columns]
    return triggers[cols].to_html(index=False, classes="data-table", escape=True)


def config_table(cfg: dict[str, Any]) -> str:
    if not cfg:
        return "<p>No config loaded.</p>"
    rows = [
        ("primaryMetric", config_value(cfg, "primaryMetric")),
        ("fixedFloor", config_value(cfg, "fixedFloor")),
        ("zScoreThreshold", config_value(cfg, "zScoreThreshold")),
        ("decisionWindowSize", config_value(cfg, "decisionWindowSize")),
        ("requiredHitsInWindow", config_value(cfg, "requiredHitsInWindow")),
        ("minBufferSamples", config_value(cfg, "minBufferSamples")),
        ("minStd", config_value(cfg, "minStd")),
        ("chronic.enabled", config_value(cfg, "chronicPolicy.enabled")),
        ("chronic.metric", config_value(cfg, "chronicPolicy.metric")),
        ("chronic.aggregator", config_value(cfg, "chronicPolicy.aggregator")),
        ("chronic.percentile", config_value(cfg, "chronicPolicy.percentile")),
        ("chronic.threshold", config_value(cfg, "chronicPolicy.threshold")),
        ("chronic.minTrafficScale", config_value(cfg, "chronicPolicy.minTrafficScale")),
    ]
    df = pd.DataFrame(rows, columns=["field", "value"])
    return df.to_html(index=False, classes="data-table", escape=True)


def fig_html(figs: list[go.Figure]) -> list[str]:
    snippets = []
    for idx, fig in enumerate(figs):
        snippets.append(pio.to_html(fig, include_plotlyjs=True if idx == 0 else False, full_html=False))
    return snippets


REPORT_TEMPLATE = Template(
    """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>NWDAF Retrain Analysis Report</title>
  <style>
    body { font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 24px; color: #172033; background: #f8fafc; }
    h1, h2 { color: #0f172a; }
    section { background: white; border: 1px solid #e2e8f0; border-radius: 12px; padding: 18px; margin: 18px 0; box-shadow: 0 1px 2px rgba(15,23,42,0.04); }
    .section-header { display: flex; align-items: center; justify-content: space-between; gap: 12px; }
    .section-header h2 { margin: 0; }
    .section-controls { white-space: nowrap; }
    .section-controls button { border: 1px solid #cbd5e1; background: #f8fafc; border-radius: 8px; padding: 4px 8px; margin-left: 4px; cursor: pointer; }
    .section-controls button:hover { background: #e2e8f0; }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 12px; }
    .card { background: #f1f5f9; border-radius: 10px; padding: 12px; }
    .card .label { color: #64748b; font-size: 12px; text-transform: uppercase; letter-spacing: .05em; }
    .card .value { font-size: 20px; font-weight: 700; margin-top: 4px; overflow-wrap: anywhere; }
    .data-table { border-collapse: collapse; width: 100%; font-size: 13px; }
    .data-table th, .data-table td { border: 1px solid #e2e8f0; padding: 6px 8px; text-align: left; vertical-align: top; }
    .data-table th { background: #e2e8f0; }
    code { background: #e2e8f0; padding: 2px 4px; border-radius: 4px; }
    .note { color: #475569; }
  </style>
</head>
<body>
  <h1>NWDAF Retrain Analysis Report</h1>

  <section data-reorderable>
    <div class="section-header"><h2>Experiment Summary</h2><div class="section-controls"><button onclick="moveSection(this, -1)">Up</button><button onclick="moveSection(this, 1)">Down</button></div></div>
    <div class="grid">
      {% for label, value in summary_cards %}
      <div class="card"><div class="label">{{ label }}</div><div class="value">{{ value }}</div></div>
      {% endfor %}
    </div>
  </section>

  <section data-reorderable>
    <div class="section-header"><h2>Config Thresholds</h2><div class="section-controls"><button onclick="moveSection(this, -1)">Up</button><button onclick="moveSection(this, 1)">Down</button></div></div>
    {{ config_table }}
  </section>

  <section data-reorderable>
    <div class="section-header"><h2>Model / Scope Aliases</h2><div class="section-controls"><button onclick="moveSection(this, -1)">Up</button><button onclick="moveSection(this, 1)">Down</button></div></div>
    <p class="note">Charts use short aliases to keep legends readable. Aliases are assigned by first observed time; full values remain available in hover text and this table.</p>
    {{ alias_table }}
  </section>

  <section data-reorderable>
    <div class="section-header"><h2>Metric Semantics</h2><div class="section-controls"><button onclick="moveSection(this, -1)">Up</button><button onclick="moveSection(this, 1)">Down</button></div></div>
    <p class="note">UL and DL are equal channel observations. Metrics are computed at channel level, not after summing UL+DL into total volume.</p>
    <ul>
      <li><code>MAE</code>: sum(abs(errUL)+abs(errDL)) / (pairCount*2)</li>
      <li><code>MSE</code>: sum(errUL^2+errDL^2) / (pairCount*2)</li>
      <li><code>WAPE</code>: sum(abs(errUL)+abs(errDL)) / sum(abs(actualUL)+abs(actualDL))</li>
      <li><code>NRMSE</code>: sqrt(MSE) / mean(abs(actualUL), abs(actualDL))</li>
      <li><code>trafficScale</code>: sum(abs(actualUL)+abs(actualDL)) / (pairCount*2), in bytes per channel observation when UPF volume is bytes</li>
    </ul>
  </section>

  <section data-reorderable>
    <div class="section-header"><h2>Metric Time Series</h2><div class="section-controls"><button onclick="moveSection(this, -1)">Up</button><button onclick="moveSection(this, 1)">Down</button></div></div>
    <p class="note">Each metric is rendered as a separate chart so the y-axis, legend, and visual scale stay local to that metric. Series are ordered by model alias, then scope alias; model aliases follow first observed time.</p>
    {{ figures.metric_series }}
  </section>

  <section data-reorderable>
    <div class="section-header"><h2>Actual vs Predicted Traffic</h2><div class="section-controls"><button onclick="moveSection(this, -1)">Up</button><button onclick="moveSection(this, 1)">Down</button></div></div>
    <p class="note">Raw traffic is reconstructed from log-derived <code>latest aggregated slot</code> and <code>ML inference</code> records. Signal and retrain lifecycle events are overlaid as hoverable markers to avoid label overlap.</p>
    {{ figures.traffic_ul }}
    {{ figures.traffic_dl }}
  </section>

  <section data-reorderable>
    <div class="section-header"><h2>Degradation Detail</h2><div class="section-controls"><button onclick="moveSection(this, -1)">Up</button><button onclick="moveSection(this, 1)">Down</button></div></div>
    <p class="note"><code>current / mean / std</code> are metric-scale values. <code>zscore</code> is shown separately because it is unitless.</p>
    {{ figures.degradation_detail }}
  </section>

  <section data-reorderable>
    <div class="section-header"><h2>Chronic Metric Detail</h2><div class="section-controls"><button onclick="moveSection(this, -1)">Up</button><button onclick="moveSection(this, 1)">Down</button></div></div>
    {{ figures.chronic_metric }}
    {{ figures.chronic_value }}
    {{ figures.chronic_traffic_scale }}
  </section>

  <section data-reorderable>
    <div class="section-header"><h2>Policy State Timeline</h2><div class="section-controls"><button onclick="moveSection(this, -1)">Up</button><button onclick="moveSection(this, 1)">Down</button></div></div>
    <p class="note"><code>degradationSignal=skipped</code> and <code>chronicSignal=skipped</code> are distinct from false and usually mean baseline is not ready while the underlying metric values are still being recorded.</p>
    {{ figures.policy_signals }}
  </section>

  <section data-reorderable>
    <div class="section-header"><h2>Decision Window Hits</h2><div class="section-controls"><button onclick="moveSection(this, -1)">Up</button><button onclick="moveSection(this, 1)">Down</button></div></div>
    {{ figures.hits }}
  </section>

  <section data-reorderable>
    <div class="section-header"><h2>Retrain Triggers</h2><div class="section-controls"><button onclick="moveSection(this, -1)">Up</button><button onclick="moveSection(this, 1)">Down</button></div></div>
    {{ trigger_table }}
  </section>

  <section data-reorderable>
    <div class="section-header"><h2>Lifecycle Events</h2><div class="section-controls"><button onclick="moveSection(this, -1)">Up</button><button onclick="moveSection(this, 1)">Down</button></div></div>
    {{ event_table }}
  </section>
  <script>
    function moveSection(button, direction) {
      const section = button.closest('section[data-reorderable]');
      if (!section) return;
      if (direction < 0) {
        const previous = section.previousElementSibling;
        if (previous && previous.matches('section[data-reorderable]')) {
          section.parentNode.insertBefore(section, previous);
          window.dispatchEvent(new Event('resize'));
        }
      } else {
        const next = section.nextElementSibling;
        if (next && next.matches('section[data-reorderable]')) {
          section.parentNode.insertBefore(next, section);
          window.dispatchEvent(new Event('resize'));
        }
      }
    }
  </script>
</body>
</html>
"""
)


def render_report(
    inputs: Inputs,
    cfg: dict[str, Any],
    metrics_df: pd.DataFrame,
    policy_df: pd.DataFrame,
    events_df: pd.DataFrame,
    traffic_df: pd.DataFrame,
    model_map: dict[str, str],
    scope_map: dict[str, str],
) -> str:
    summary = make_summary(metrics_df, policy_df, events_df, traffic_df, inputs)
    primary_metric = config_value(cfg, "primaryMetric", "MAE")
    chronic_metric = config_value(cfg, "chronicPolicy.metric", "WAPE")
    fixed_floor = parse_float(config_value(cfg, "fixedFloor"))
    chronic_threshold = parse_float(config_value(cfg, "chronicPolicy.threshold"))

    metric_figures = metric_series_charts(metrics_df, primary_metric, fixed_floor)
    figures = metric_figures + [
        metric_chart(metrics_df, f"Chronic Metric: {chronic_metric}", chronic_metric, chronic_threshold),
        degradation_detail_chart(policy_df, cfg),
        chronic_traffic_scale_chart(policy_df, cfg),
        chronic_value_chart(policy_df, cfg),
        traffic_timeline_chart(traffic_df, policy_df, events_df, "UL"),
        traffic_timeline_chart(traffic_df, policy_df, events_df, "DL"),
        policy_signal_chart(policy_df),
        hits_chart(policy_df),
    ]
    snippets = fig_html(figures)
    metric_count = len(metric_figures)
    detail_snippets = snippets[metric_count:]

    cards = [
        ("time start", summary["timeStart"]),
        ("time end", summary["timeEnd"]),
        ("models", summary["models"]),
        ("scopes", summary["scopes"]),
        ("metrics rows", summary["metricsRows"]),
        ("policy rows", summary["policyRows"]),
        ("traffic rows", summary["trafficRows"]),
        ("retrain triggers", summary["events"].get("retrain_trigger", 0)),
        ("hot swaps", summary["events"].get("hot_swap_complete", 0)),
    ]

    return REPORT_TEMPLATE.render(
        summary_cards=[(label, html.escape(str(value))) for label, value in cards],
        config_table=config_table(cfg),
        alias_table=alias_table(model_map, scope_map),
        event_table=event_table(events_df),
        trigger_table=trigger_table(events_df, policy_df),
        figures={
            "metric_series": "\n".join(snippets[:metric_count]),
            "chronic_metric": detail_snippets[0],
            "degradation_detail": detail_snippets[1],
            "chronic_traffic_scale": detail_snippets[2],
            "chronic_value": detail_snippets[3],
            "traffic_ul": detail_snippets[4],
            "traffic_dl": detail_snippets[5],
            "policy_signals": detail_snippets[6],
            "hits": detail_snippets[7],
        },
    )


def main() -> None:
    args = parse_args()
    inputs = discover_inputs(args)
    cfg = read_accuracy_config(inputs.config)
    metrics_df, policy_df, events_df, traffic_df = parse_log(inputs.log)
    model_map, scope_map = build_alias_maps([metrics_df, policy_df, events_df, traffic_df])
    metrics_df = apply_aliases(metrics_df, model_map, scope_map)
    policy_df = apply_aliases(policy_df, model_map, scope_map)
    events_df = apply_aliases(events_df, model_map, scope_map)
    traffic_df = apply_aliases(traffic_df, model_map, scope_map)

    inputs.out.parent.mkdir(parents=True, exist_ok=True)
    inputs.out.write_text(
        render_report(inputs, cfg, metrics_df, policy_df, events_df, traffic_df, model_map, scope_map),
        encoding="utf-8",
    )
    print(f"Wrote {inputs.out}")
    print(
        f"metrics_rows={len(metrics_df)} policy_rows={len(policy_df)} "
        f"events_rows={len(events_df)} traffic_rows={len(traffic_df)}"
    )


if __name__ == "__main__":
    main()
