#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import io
import json
import math
import os
import shutil
import socket
import subprocess
import sys
import tarfile
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import joblib
import numpy as np
import pandas as pd
import requests
import torch
import yaml

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_FEATURE_ORDER = [
    "total_vol",
    "ul_vol",
    "dl_vol",
    "total_nb_pkts",
    "ul_nb_pkts",
    "dl_nb_pkts",
    "ul_thr",
    "dl_thr",
    "ul_pkt_thr",
    "dl_pkt_thr",
]
DEFAULT_OUTPUT_FIELDS = ["ul_vol", "dl_vol"]
TRAFFIC_SCALE_METRIC = "__traffic_scale__"
PREDICTED_TRAFFIC_SCALE_METRIC = "__predicted_traffic_scale__"
LOW_TRAFFIC_OVERSHOOT_EPSILON_BASE = 1.0


def now_utc() -> pd.Timestamp:
    return pd.Timestamp.now(tz="UTC")


def console_log(message: str) -> None:
    ts = now_utc().strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[retrain-replay] {ts} {message}", flush=True)


def compose_hit_reason(degradation_hit: bool, chronic_hit: bool, low_traffic_hit: bool) -> str:
    reasons: list[str] = []
    if degradation_hit:
        reasons.append("degradation")
    if chronic_hit:
        reasons.append("chronic")
    if low_traffic_hit:
        reasons.append("low_traffic_overprediction")
    return "+".join(reasons) if reasons else "none"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Offline NWDAF retrain replay tool.")
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="Run replay and emit structured trace.")
    run.add_argument("--dataset-root", type=Path, required=True)
    run.add_argument("--config", type=Path, required=True)
    run.add_argument("--replay-config", type=Path, required=True)
    run.add_argument("--initial-bundle", type=Path, required=True)
    run.add_argument("--daisy-example-dir", type=Path, required=True)
    run.add_argument("--out", type=Path, required=True)
    run.add_argument("--groups", nargs="*", help="Override dataset groups.")
    run.add_argument("--skip-daisy", action="store_true")
    return parser.parse_args()


def config_value(cfg: dict[str, Any], path: str, default: Any = None) -> Any:
    cur: Any = cfg
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur


def parse_int(value: Any, default: int | None = None) -> int | None:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def parse_float(value: Any, default: float | None = None) -> float | None:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def isoformat_utc(ts: pd.Timestamp) -> str:
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return ts.isoformat().replace("+00:00", "Z")


def format_bps(value: float) -> str:
    return f"{value:.0f} bps"


def format_pps(value: float) -> str:
    return f"{value:.2f} pps"


def parse_direction(value: Any) -> str:
    text = str(value).strip().lower()
    if text in {"0", "ul", "uplink"}:
        return "ul"
    if text in {"1", "dl", "downlink"}:
        return "dl"
    return "unknown"


def compute_mae(pairs: list[tuple[int, int, int, int]]) -> float:
    if not pairs:
        return 0.0
    total = 0.0
    for pred_ul, pred_dl, actual_ul, actual_dl in pairs:
        total += abs(pred_ul - actual_ul)
        total += abs(pred_dl - actual_dl)
    return total / (len(pairs) * 2)


def compute_mse(pairs: list[tuple[int, int, int, int]]) -> float:
    if not pairs:
        return 0.0
    total = 0.0
    for pred_ul, pred_dl, actual_ul, actual_dl in pairs:
        total += (pred_ul - actual_ul) ** 2
        total += (pred_dl - actual_dl) ** 2
    return total / (len(pairs) * 2)


def compute_smape(pairs: list[tuple[int, int, int, int]]) -> float:
    if not pairs:
        return 0.0
    total = 0.0
    count = 0
    for pred_ul, pred_dl, actual_ul, actual_dl in pairs:
        ul_denom = abs(pred_ul) + abs(actual_ul)
        dl_denom = abs(pred_dl) + abs(actual_dl)
        if ul_denom > 0:
            total += abs(pred_ul - actual_ul) / (ul_denom / 2)
            count += 1
        if dl_denom > 0:
            total += abs(pred_dl - actual_dl) / (dl_denom / 2)
            count += 1
    return total / count if count else 0.0


def sum_abs_actual(pairs: list[tuple[int, int, int, int]]) -> float:
    total = 0.0
    for _, _, actual_ul, actual_dl in pairs:
        total += abs(actual_ul)
        total += abs(actual_dl)
    return total


def mean_abs_actual(pairs: list[tuple[int, int, int, int]]) -> float:
    if not pairs:
        return 0.0
    return sum_abs_actual(pairs) / (len(pairs) * 2)


def sum_abs_pred(pairs: list[tuple[int, int, int, int]]) -> float:
    total = 0.0
    for pred_ul, pred_dl, _, _ in pairs:
        total += abs(pred_ul)
        total += abs(pred_dl)
    return total


def mean_abs_pred(pairs: list[tuple[int, int, int, int]]) -> float:
    if not pairs:
        return 0.0
    return sum_abs_pred(pairs) / (len(pairs) * 2)


def compute_wape(pairs: list[tuple[int, int, int, int]]) -> float:
    if not pairs:
        return 0.0
    total_err = 0.0
    for pred_ul, pred_dl, actual_ul, actual_dl in pairs:
        total_err += abs(pred_ul - actual_ul)
        total_err += abs(pred_dl - actual_dl)
    total_actual = sum_abs_actual(pairs)
    return total_err / total_actual if total_actual > 0 else 0.0


def compute_nrmse(pairs: list[tuple[int, int, int, int]]) -> float:
    if not pairs:
        return 0.0
    denom = mean_abs_actual(pairs)
    return math.sqrt(compute_mse(pairs)) / denom if denom > 0 else 0.0


def compute_metrics(pairs: list[tuple[int, int, int, int]]) -> dict[str, float]:
    return {
        "sMAPE": compute_smape(pairs),
        "MAE": compute_mae(pairs),
        "MSE": compute_mse(pairs),
        "WAPE": compute_wape(pairs),
        "NRMSE": compute_nrmse(pairs),
    }


@dataclass
class PolicyObservation:
    timestamp: pd.Timestamp
    sample_count: int = 0
    traffic_scale: float = 0.0
    predicted_traffic_scale: float = 0.0
    metrics: dict[str, float] = field(default_factory=dict)


class ObservationBuffer:
    def __init__(self, size: int):
        self.size = max(1, size)
        self.observations: deque[PolicyObservation] = deque(maxlen=self.size)

    def add(self, observation: PolicyObservation) -> None:
        self.observations.append(
            PolicyObservation(
                timestamp=observation.timestamp,
                sample_count=observation.sample_count,
                traffic_scale=observation.traffic_scale,
                predicted_traffic_scale=observation.predicted_traffic_scale,
                metrics=dict(observation.metrics),
            )
        )

    def count(self) -> int:
        return len(self.observations)

    def snapshot(self) -> list[PolicyObservation]:
        return [
            PolicyObservation(
                timestamp=observation.timestamp,
                sample_count=observation.sample_count,
                traffic_scale=observation.traffic_scale,
                predicted_traffic_scale=observation.predicted_traffic_scale,
                metrics=dict(observation.metrics),
            )
            for observation in self.observations
        ]

    def metric_values(self, metric: str) -> list[float]:
        values: list[float] = []
        for observation in self.snapshot():
            if metric == TRAFFIC_SCALE_METRIC:
                values.append(float(observation.traffic_scale))
                continue
            if metric == PREDICTED_TRAFFIC_SCALE_METRIC:
                values.append(float(observation.predicted_traffic_scale))
                continue
            if metric in observation.metrics:
                values.append(float(observation.metrics[metric]))
        return values


class HitWindow:
    def __init__(self, size: int):
        self.size = max(1, size)
        self.values: deque[bool] = deque(maxlen=self.size)

    def add(self, hit: bool) -> int:
        self.values.append(bool(hit))
        return sum(1 for value in self.values if value)

    def reset(self) -> None:
        self.values.clear()

    def true_count(self) -> int:
        return sum(1 for value in self.values if value)


class ScopeState:
    def __init__(self, buffer_size: int, decision_window_size: int):
        self.recent_observations = ObservationBuffer(buffer_size)
        self.degradation_reference = ObservationBuffer(buffer_size)
        self.degradation_window = HitWindow(decision_window_size)
        self.chronic_window = HitWindow(decision_window_size)
        self.low_traffic_window = HitWindow(decision_window_size)
        self.buffer_size = buffer_size
        self.last_update: pd.Timestamp | None = None

    def record_observation(self, observation: PolicyObservation) -> None:
        self.recent_observations.add(observation)
        self.last_update = observation.timestamp

    def record_degradation_reference(self, observation: PolicyObservation) -> None:
        self.degradation_reference.add(observation)
        self.last_update = observation.timestamp

    def record_metric(self, metric: str, value: float, now: pd.Timestamp) -> None:
        observation = PolicyObservation(timestamp=now)
        if metric == TRAFFIC_SCALE_METRIC:
            observation.traffic_scale = value
        elif metric == PREDICTED_TRAFFIC_SCALE_METRIC:
            observation.predicted_traffic_scale = value
        else:
            observation.metrics[metric] = value
        self.record_observation(observation)

    def values(self, metric: str) -> list[float]:
        return self.recent_observations.metric_values(metric)

    def degradation_values(self, metric: str) -> list[float]:
        return self.degradation_reference.metric_values(metric)

    def sample_count(self, metric: str) -> int:
        return len(self.values(metric))

    def degradation_sample_count(self, metric: str) -> int:
        return len(self.degradation_values(metric))

    def mean(self, metric: str) -> float:
        values = self.values(metric)
        return float(np.mean(values)) if values else 0.0

    def degradation_mean(self, metric: str) -> float:
        values = self.degradation_values(metric)
        return float(np.mean(values)) if values else 0.0

    def std(self, metric: str) -> float:
        values = self.values(metric)
        return float(np.std(values)) if values else 0.0

    def degradation_std(self, metric: str) -> float:
        values = self.degradation_values(metric)
        return float(np.std(values)) if values else 0.0

    def percentile(self, metric: str, percentile: int) -> float:
        values = self.values(metric)
        if not values:
            return 0.0
        position = max(0.0, min(100.0, float(percentile)))
        return float(np.percentile(np.array(values, dtype=float), position))

    def record_degradation_outcome(self, hit: bool) -> int:
        return self.degradation_window.add(hit)

    def record_chronic_outcome(self, hit: bool) -> int:
        return self.chronic_window.add(hit)

    def record_low_traffic_outcome(self, hit: bool) -> int:
        return self.low_traffic_window.add(hit)

    def reset_decision_windows(self) -> None:
        self.degradation_window.reset()
        self.chronic_window.reset()
        self.low_traffic_window.reset()

    def reset_degradation_window(self) -> None:
        self.degradation_window.reset()

    def reset_chronic_window(self) -> None:
        self.chronic_window.reset()

    def reset_low_traffic_window(self) -> None:
        self.low_traffic_window.reset()


class MonitorStateStore:
    def __init__(self) -> None:
        self.scopes: dict[str, ScopeState] = {}

    def get_or_create_scope(self, scope_key: str, buffer_size: int, decision_window_size: int) -> ScopeState:
        scope = self.scopes.get(scope_key)
        if scope is None:
            scope = ScopeState(buffer_size, decision_window_size)
            self.scopes[scope_key] = scope
        return scope

    def reset_all(self) -> None:
        for scope in self.scopes.values():
            scope.reset_decision_windows()


@dataclass
class SlotObservation:
    group_id: str
    window_index: int
    slot_start: pd.Timestamp
    slot_end: pd.Timestamp
    ul_vol: int
    dl_vol: int
    ul_pkts: int
    dl_pkts: int
    ul_thr: float
    dl_thr: float
    ul_pkt_thr: float
    dl_pkt_thr: float
    notification_doc: dict[str, Any]
    ue_notification_docs: list[dict[str, Any]] = field(default_factory=list)

    @property
    def feature_row(self) -> dict[str, float]:
        return {
            "total_vol": float(self.ul_vol + self.dl_vol),
            "ul_vol": float(self.ul_vol),
            "dl_vol": float(self.dl_vol),
            "total_nb_pkts": float(self.ul_pkts + self.dl_pkts),
            "ul_nb_pkts": float(self.ul_pkts),
            "dl_nb_pkts": float(self.dl_pkts),
            "ul_thr": float(self.ul_thr),
            "dl_thr": float(self.dl_thr),
            "ul_pkt_thr": float(self.ul_pkt_thr),
            "dl_pkt_thr": float(self.dl_pkt_thr),
        }


@dataclass
class GroupReplayData:
    group_id: str
    slots: list[SlotObservation]
    ue_windows: list["UeWindowObservation"]
    breaking_time_sec: float
    aligned_breaking_time_sec: int


@dataclass
class UeWindowObservation:
    group_id: str
    ue_ip: str
    window_index: int
    slot_start: pd.Timestamp
    slot_end: pd.Timestamp
    ul_vol: int
    dl_vol: int
    ul_pkts: int
    dl_pkts: int
    ul_thr: float
    dl_thr: float
    ul_pkt_thr: float
    dl_pkt_thr: float
    notification_doc: dict[str, Any]


@dataclass
class ModelVersion:
    key: str
    bundle_dir: Path
    bundle_model_path: Path
    bundle_scaler_path: Path
    model_id: str
    model: Any
    scaler: Any
    input_window: int
    feature_order: list[str]
    output_fields: list[str]
    source: str
    daisy_tid: str | None = None
    daisy_local_model_path: Path | None = None
    daisy_local_scaler_path: Path | None = None


@dataclass
class PredictionRecord:
    prediction_id: str
    group_id: str
    scope: str
    model_version: str
    model_source: str
    predicted_at_sim_time: pd.Timestamp
    base_target_sim_time: pd.Timestamp
    target_sim_time: pd.Timestamp
    target_slot_sim_time: pd.Timestamp
    matured_at_sim_time: pd.Timestamp
    pred_ul: int
    pred_dl: int
    confidence: int
    miss_count: int = 0
    matched_actual_ul: int | None = None
    matched_actual_dl: int | None = None
    matched_actual_slot_start: pd.Timestamp | None = None
    resolved_round_index: int | None = None
    resolved_at_sim_time: pd.Timestamp | None = None
    discarded: bool = False
    discard_reason: str | None = None


class PendingPredictionStore:
    def __init__(self) -> None:
        self.predictions: list[PredictionRecord] = []

    def add_prediction(self, record: PredictionRecord) -> None:
        self.predictions.append(record)

    def snapshot_predictions(self, group_id: str | None = None) -> list[PredictionRecord]:
        if group_id is None:
            return list(self.predictions)
        return [pred for pred in self.predictions if pred.group_id == group_id]

    def resolve_predictions(
        self,
        matched_ids: set[str],
        missed_ids: set[str],
        max_miss_count: int,
        round_index: int,
        round_time: pd.Timestamp,
    ) -> tuple[int, int, list[PredictionRecord]]:
        if max_miss_count <= 0:
            max_miss_count = 1

        updated: list[PredictionRecord] = []
        matched_count = 0
        discarded_count = 0
        discarded_predictions: list[PredictionRecord] = []
        for pred in self.predictions:
            if pred.prediction_id in matched_ids:
                pred.resolved_round_index = round_index
                pred.resolved_at_sim_time = round_time
                matched_count += 1
                continue
            if pred.prediction_id in missed_ids:
                pred.miss_count += 1
                if pred.miss_count >= max_miss_count:
                    pred.discarded = True
                    pred.discard_reason = "max_miss_count"
                    pred.resolved_round_index = round_index
                    pred.resolved_at_sim_time = round_time
                    discarded_count += 1
                    discarded_predictions.append(pred)
                    continue
            updated.append(pred)
        self.predictions = updated
        return matched_count, discarded_count, discarded_predictions

    def discard_all_predictions(
        self,
        *,
        group_id: str | None,
        discard_reason: str,
        round_time: pd.Timestamp,
    ) -> list[PredictionRecord]:
        discarded: list[PredictionRecord] = []
        updated: list[PredictionRecord] = []
        for pred in self.predictions:
            if group_id is not None and pred.group_id != group_id:
                updated.append(pred)
                continue
            pred.discarded = True
            pred.discard_reason = discard_reason
            pred.resolved_at_sim_time = round_time
            discarded.append(pred)
        self.predictions = updated
        return discarded

    def discard_predictions_before(
        self,
        sim_time: pd.Timestamp,
        *,
        discard_reason: str,
    ) -> list[PredictionRecord]:
        discarded: list[PredictionRecord] = []
        updated: list[PredictionRecord] = []
        for pred in self.predictions:
            if pred.target_sim_time >= sim_time:
                updated.append(pred)
                continue
            pred.discarded = True
            pred.discard_reason = discard_reason
            pred.resolved_at_sim_time = sim_time
            discarded.append(pred)
        self.predictions = updated
        return discarded


@dataclass
class PendingActivation:
    effective_sim_time: pd.Timestamp
    model_version: ModelVersion
    retrain_job: dict[str, Any]


@dataclass
class StartupTimingProfile:
    group_id: str
    mode: str
    first_data_slot_epoch: pd.Timestamp
    first_target_slot_epoch: pd.Timestamp
    first_urr_signal_epoch: pd.Timestamp
    anchor_established_epoch: pd.Timestamp
    first_visible_inference_epoch: pd.Timestamp
    subscription_epoch: pd.Timestamp
    warmup_end_epoch: pd.Timestamp
    first_monitor_epoch: pd.Timestamp
    subscription_to_first_visible_inference_sec: int
    startup_warmup_sec: int
    prediction_visibility_lag_sec: int
    component_offsets_sec: dict[str, int] = field(default_factory=dict)


class DaisyCallbackServer:
    def __init__(self, host: str, port: int | None):
        self.host = host
        self.port = port or 0
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None
        self._results: dict[str, dict[str, Any]] = {}
        self._cond = threading.Condition()

    def start(self) -> None:
        if self._server is not None:
            return
        parent = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                length = int(self.headers.get("Content-Length", "0"))
                raw = self.rfile.read(length) if length > 0 else b"{}"
                payload = json.loads(raw.decode("utf-8") or "{}")
                task_id = payload.get("task_id")
                if task_id:
                    with parent._cond:
                        parent._results[str(task_id)] = payload
                        parent._cond.notify_all()
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"ok")

            def log_message(self, format: str, *args: Any) -> None:
                return

        self._server = ThreadingHTTPServer((self.host, self.port), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    @property
    def address(self) -> str:
        if self._server is None:
            raise RuntimeError("Callback server not started")
        host, port = self._server.server_address[:2]
        return f"http://{host}:{port}/training-complete"

    def wait_for_task(self, task_id: str, timeout_sec: int) -> dict[str, Any]:
        deadline = time.time() + timeout_sec
        with self._cond:
            while task_id not in self._results:
                remaining = deadline - time.time()
                if remaining <= 0:
                    raise TimeoutError(f"Timed out waiting for Daisy callback for task {task_id}")
                self._cond.wait(timeout=remaining)
            return self._results.pop(task_id)

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None


class DaisyManager:
    def __init__(self, example_dir: Path, endpoint: str, task_cfg: dict[str, Any], auto_manage: bool, reuse_existing: bool, python_bin: str, timeout_sec: int, callback_host: str, callback_port: int | None, out_dir: Path):
        self.example_dir = example_dir
        self.endpoint = endpoint.rstrip("/")
        self.task_cfg = task_cfg
        self.auto_manage = auto_manage
        self.reuse_existing = reuse_existing
        self.python_bin = python_bin
        self.timeout_sec = timeout_sec
        self.out_dir = out_dir
        self.master_proc: subprocess.Popen[str] | None = None
        self.session = requests.Session()
        self.callback_server = DaisyCallbackServer(callback_host, callback_port)

    def _is_alive(self) -> bool:
        try:
            resp = self.session.get(f"{self.endpoint}/metrics", timeout=2)
            return resp.status_code < 500
        except Exception:
            return False

    def ensure_started(self) -> None:
        self.callback_server.start()
        if self.reuse_existing and self._is_alive():
            console_log(f"daisy reuse existing endpoint={self.endpoint}")
            return
        if not self.auto_manage:
            raise RuntimeError("Daisy endpoint is not reachable and auto_manage=false")
        if self._is_alive():
            console_log(f"daisy endpoint already reachable endpoint={self.endpoint}")
            return
        parsed = urlparse(self.endpoint)
        endpoint_host = parsed.hostname or "127.0.0.1"
        endpoint_port = parsed.port or 9887
        log_path = self.out_dir / "daisy_master.log"
        log_file = log_path.open("w", encoding="utf-8")
        daisy_src_py = self.example_dir.parents[1] / "src" / "py"
        env = os.environ.copy()
        existing_pythonpath = env.get("PYTHONPATH", "")
        env["PYTHONPATH"] = (
            f"{daisy_src_py}:{existing_pythonpath}" if existing_pythonpath else str(daisy_src_py)
        )
        cmd = [
            self.python_bin,
            "master.py",
            "--server_address",
            "0.0.0.0:8887",
            "--api_ip",
            endpoint_host,
            "--api_port",
            str(endpoint_port),
            "--init_model",
            "--data_dirs",
            "ees_training_data",
        ]
        console_log(
            f"starting daisy master endpoint={self.endpoint} python={self.python_bin} "
            f"log={log_path} py_path={env['PYTHONPATH']}"
        )
        self.master_proc = subprocess.Popen(
            cmd,
            cwd=self.example_dir,
            env=env,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            text=True,
        )
        deadline = time.time() + 30
        while time.time() < deadline:
            if self._is_alive():
                console_log(f"daisy master ready endpoint={self.endpoint}")
                return
            time.sleep(1)
        raise RuntimeError("Timed out waiting for Daisy master to start")

    def upload_notifications(self, tid: str, group_id: str, docs: list[dict[str, Any]], upload_batch_size: int) -> None:
        for start in range(0, len(docs), upload_batch_size):
            chunk = docs[start : start + upload_batch_size]
            payload = {"tid": tid, "group_id": group_id, "data": chunk}
            resp = self.session.post(f"{self.endpoint}/upload_data", json=payload, timeout=self.timeout_sec)
            resp.raise_for_status()

    def publish_task(
        self,
        tid: str,
        *,
        use_fixed_scaler: bool = False,
        seed_model_path: Path | None = None,
        seed_scaler_path: Path | None = None,
    ) -> None:
        payload = json.loads(json.dumps(self.task_cfg))
        payload["TID"] = tid
        payload["CALLBACK_URL"] = self.callback_server.address
        if use_fixed_scaler:
            payload["USE_FIXED_SCALER"] = True
        if seed_model_path is not None:
            payload["SEED_MODEL_PATH"] = str(seed_model_path)
        if seed_scaler_path is not None:
            payload["SEED_SCALER_PATH"] = str(seed_scaler_path)
        console_log(
            f"daisy publish_task tid={tid} fixed_scaler={use_fixed_scaler} "
            f"seed_model={seed_model_path} seed_scaler={seed_scaler_path}"
        )
        resp = self.session.post(f"{self.endpoint}/publish_task", json=payload, timeout=self.timeout_sec)
        resp.raise_for_status()

    def wait_for_result(self, tid: str) -> dict[str, Any]:
        return self.callback_server.wait_for_task(tid, self.timeout_sec)

    def download_artifact(self, model_url: str, dest_dir: Path) -> Path:
        url = model_url.replace("http://0.0.0.0:", "http://127.0.0.1:")
        resp = self.session.get(url, timeout=self.timeout_sec)
        resp.raise_for_status()
        dest_dir.mkdir(parents=True, exist_ok=True)
        with tarfile.open(fileobj=io.BytesIO(resp.content), mode="r:gz") as tar:
            tar.extractall(dest_dir)
        return dest_dir

    def stop(self) -> None:
        self.callback_server.stop()
        if self.master_proc is not None:
            self.master_proc.terminate()
            try:
                self.master_proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.master_proc.kill()


class ReplayEngine:
    def __init__(
        self,
        nw_cfg: dict[str, Any],
        replay_cfg: dict[str, Any],
        initial_bundle: Path,
        daisy_example_dir: Path,
        out_dir: Path,
        skip_daisy: bool,
        dataset_root: Path,
    ):
        self.nw_cfg = nw_cfg
        self.replay_cfg = replay_cfg
        self.initial_bundle = initial_bundle
        self.daisy_example_dir = daisy_example_dir
        self.out_dir = out_dir
        self.skip_daisy = skip_daisy
        self.dataset_root = dataset_root

        self.sampling_interval = parse_int(config_value(nw_cfg, "configuration.analytics.ueCommunication.samplingInterval"), parse_int(config_value(replay_cfg, "dataset.report_period_sec"), 5)) or 5
        self.report_period_sec = parse_int(config_value(replay_cfg, "dataset.report_period_sec"), self.sampling_interval) or self.sampling_interval
        self.check_interval_sec = parse_int(config_value(replay_cfg, "monitor.check_interval_sec"), parse_int(config_value(nw_cfg, "configuration.mtlf.accuracyMonitor.checkInterval"), 60)) or 60
        self.maturity_lag_sec = parse_int(config_value(replay_cfg, "monitor.maturity_lag_sec"), self.sampling_interval * 2) or (self.sampling_interval * 2)
        self.startup_timing_cfg = config_value(replay_cfg, "startup_timing", {}) or {}
        self.startup_timing_mode = str(self.startup_timing_cfg.get("mode") or "legacy_single_offset").strip() or "legacy_single_offset"
        self.subscription_to_first_urr_delay_sec = (
            parse_int(
                config_value(replay_cfg, "dataset.subscription_to_first_urr_delay_sec"),
                parse_int(config_value(replay_cfg, "dataset.first_urr_delay_sec"), 0),
            )
            or 0
        )
        raw_group_delays = (
            config_value(replay_cfg, "dataset.subscription_to_first_urr_delay_sec_by_group", {})
            or config_value(replay_cfg, "dataset.first_urr_delay_sec_by_group", {})
            or {}
        )
        self.subscription_to_first_urr_delay_sec_by_group: dict[str, int] = {}
        if isinstance(raw_group_delays, dict):
            for group_id, raw_value in raw_group_delays.items():
                parsed = parse_int(raw_value)
                if parsed is not None:
                    self.subscription_to_first_urr_delay_sec_by_group[str(group_id)] = parsed
        self.startup_subscription_to_first_urr_signal_sec = parse_int(
            self.startup_timing_cfg.get("subscription_to_first_urr_signal_sec"),
            0,
        ) or 0
        self.startup_first_urr_signal_to_anchor_sec = parse_int(
            self.startup_timing_cfg.get("first_urr_signal_to_anchor_sec"),
            0,
        ) or 0
        self.startup_anchor_to_first_visible_inference_sec = parse_int(
            self.startup_timing_cfg.get("anchor_to_first_visible_inference_sec"),
            0,
        ) or 0
        self.retrain_window_sec = parse_int(config_value(replay_cfg, "retrain.window_sec"), parse_int(config_value(nw_cfg, "configuration.adrf.retrainWindow"), 1800)) or 1800
        self.upload_batch_size = parse_int(config_value(replay_cfg, "retrain.upload_batch_size"), 500) or 500
        self.mock_training_duration_sec = parse_int(config_value(replay_cfg, "retrain.mock_training_duration_sec"), 120) or 120
        self.progress_every_slots = parse_int(config_value(replay_cfg, "report.progress_every_slots"), 120) or 120

        self.acc_cfg = config_value(nw_cfg, "configuration.mtlf.accuracyMonitor", {}) or {}
        self.metrics_to_record = list(self.acc_cfg.get("metricsToRecord") or ["sMAPE", "MAE", "MSE", "WAPE", "NRMSE"])
        self.primary_metric = self.acc_cfg.get("primaryMetric") or "MAE"
        self.buffer_size = parse_int(self.acc_cfg.get("recentBufferSize"), 20) or 20
        self.min_buffer_samples = parse_int(self.acc_cfg.get("minBufferSamples"), 8) or 8
        self.min_std = parse_float(self.acc_cfg.get("minStd"), 0.01) or 0.01
        self.fixed_floor = parse_float(self.acc_cfg.get("fixedFloor"), 1024.0) or 1024.0
        self.z_threshold = parse_float(self.acc_cfg.get("zScoreThreshold"), 3.0) or 3.0
        self.window_size = parse_int(self.acc_cfg.get("decisionWindowSize"), 3) or 3
        self.required_hits = parse_int(self.acc_cfg.get("requiredHitsInWindow"), 3) or 3
        self.min_samples = parse_int(self.acc_cfg.get("minSamples"), 5) or 5
        self.startup_warmup_sec = parse_int(self.acc_cfg.get("WarmupDuration"), parse_int(self.acc_cfg.get("warmupDuration"), 120)) or 120
        self.chronic_cfg = self.acc_cfg.get("chronicPolicy") or {}
        self.low_traffic_cfg = self.acc_cfg.get("lowTrafficOverpredictionPolicy") or {}

        mtlf_cfg = config_value(nw_cfg, "configuration.mtlf", {}) or {}
        self.task_cfg = mtlf_cfg.get("task") or {}
        self.use_fixed_scaler = bool(config_value(replay_cfg, "daisy.use_fixed_scaler", False))
        self.enable_continue_learning = bool(config_value(replay_cfg, "daisy.enable_continue_learning", False))
        endpoint = (mtlf_cfg.get("endpoint") or "http://127.0.0.1:9887").rstrip("/")
        self.daisy = DaisyManager(
            daisy_example_dir,
            endpoint,
            self.task_cfg,
            auto_manage=bool(config_value(replay_cfg, "daisy.auto_manage", True)),
            reuse_existing=bool(config_value(replay_cfg, "daisy.reuse_existing", False)),
            python_bin=str(config_value(replay_cfg, "daisy.python_bin", "python3.8")),
            timeout_sec=parse_int(config_value(replay_cfg, "daisy.publish_timeout_sec"), 3600) or 3600,
            callback_host=str(config_value(replay_cfg, "daisy.callback_host", "127.0.0.1")),
            callback_port=parse_int(config_value(replay_cfg, "daisy.callback_port"), None),
            out_dir=out_dir,
        )

        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.current_model = self.load_bundle(initial_bundle, "initial", source="initial_bundle")
        self.fixed_scaler_seed_path = self.current_model.bundle_scaler_path if self.use_fixed_scaler else None
        self.pending_activation: PendingActivation | None = None
        self.retraining_until: pd.Timestamp | None = None
        self.history: dict[str, deque[dict[str, float]]] = {}
        self.actual_slots_by_group_time: dict[tuple[str, pd.Timestamp], SlotObservation] = {}
        self.slot_observations: list[SlotObservation] = []
        self.prediction_store = PendingPredictionStore()
        self.monitor_state = MonitorStateStore()
        self.inference_since_last_monitor: dict[str, int] = {}

        self.slots_rows: list[dict[str, Any]] = []
        self.prediction_rows: list[dict[str, Any]] = []
        self.prediction_rows_by_id: dict[str, dict[str, Any]] = {}
        self.monitor_rows: list[dict[str, Any]] = []
        self.policy_rows: list[dict[str, Any]] = []
        self.retrain_rows: list[dict[str, Any]] = []
        self.event_rows: list[dict[str, Any]] = []
        self.processed_live_slots = 0
        self.total_live_slots = 0
        self.next_progress_slot = self.progress_every_slots
        self.monitor_round_count = 0
        self.startup_warmup_done_groups: set[str] = set()
        self.pending_warmup_discard_counts: dict[str, int] = {}
        self.pending_warmup_discard_applied_groups: set[str] = set()
        self.prediction_target_offset_sec = self.maturity_lag_sec
        self.prediction_target_mode = str(
            config_value(replay_cfg, "monitor.prediction_target_mode", "historical_next_slot")
        ).strip() or "historical_next_slot"
        self.startup_profile_by_group: dict[str, StartupTimingProfile] = {}
        self.prediction_visibility_lag_sec_by_group: dict[str, int] = {}
        self.scheduled_predictions: list[PredictionRecord] = []

    def log_progress(self, message: str) -> None:
        console_log(message)

    def build_startup_timing_profile(
        self,
        group_id: str,
        first_slot: SlotObservation,
        startup_warmup: int,
    ) -> StartupTimingProfile:
        first_data_slot_epoch = first_slot.slot_start
        first_target_slot_epoch = self.derive_prediction_base_target_time(first_slot)

        if self.startup_timing_mode == "explicit_offsets":
            subscription_to_first_urr_signal_sec = self.startup_subscription_to_first_urr_signal_sec
            first_urr_signal_to_anchor_sec = self.startup_first_urr_signal_to_anchor_sec
            anchor_to_first_visible_inference_sec = self.startup_anchor_to_first_visible_inference_sec
            component_offsets_sec = {
                "subscriptionToFirstUrrSignalSec": subscription_to_first_urr_signal_sec,
                "firstUrrSignalToAnchorSec": first_urr_signal_to_anchor_sec,
                "anchorToFirstVisibleInferenceSec": anchor_to_first_visible_inference_sec,
            }
            subscription_to_first_visible_inference_sec = sum(component_offsets_sec.values())
            subscription_epoch = first_target_slot_epoch - pd.Timedelta(seconds=subscription_to_first_urr_signal_sec)
            first_urr_signal_epoch = subscription_epoch + pd.Timedelta(seconds=subscription_to_first_urr_signal_sec)
            anchor_established_epoch = first_urr_signal_epoch + pd.Timedelta(seconds=first_urr_signal_to_anchor_sec)
            first_visible_inference_epoch = anchor_established_epoch + pd.Timedelta(
                seconds=anchor_to_first_visible_inference_sec
            )
        else:
            legacy_delay_sec = self.delay_for_group(group_id)
            slot_visibility_lag_sec = int((first_slot.slot_end - first_slot.slot_start).total_seconds())
            component_offsets_sec = {
                "legacySubscriptionToFirstUrrDelaySec": legacy_delay_sec,
                "slotVisibilityLagSec": slot_visibility_lag_sec,
            }
            subscription_to_first_visible_inference_sec = legacy_delay_sec + slot_visibility_lag_sec
            first_visible_inference_epoch = first_data_slot_epoch + pd.Timedelta(
                seconds=subscription_to_first_visible_inference_sec
            )
            subscription_epoch = first_visible_inference_epoch - pd.Timedelta(
                seconds=subscription_to_first_visible_inference_sec
            )
            first_urr_signal_epoch = subscription_epoch + pd.Timedelta(seconds=legacy_delay_sec)
            anchor_established_epoch = first_urr_signal_epoch

        warmup_end_epoch = subscription_epoch + pd.Timedelta(seconds=startup_warmup)
        first_monitor_epoch = warmup_end_epoch + pd.Timedelta(seconds=self.check_interval_sec)
        prediction_visibility_lag_sec = max(
            0,
            int((first_visible_inference_epoch - first_target_slot_epoch).total_seconds()),
        )
        return StartupTimingProfile(
            group_id=group_id,
            mode=self.startup_timing_mode,
            first_data_slot_epoch=first_data_slot_epoch,
            first_target_slot_epoch=first_target_slot_epoch,
            first_urr_signal_epoch=first_urr_signal_epoch,
            anchor_established_epoch=anchor_established_epoch,
            first_visible_inference_epoch=first_visible_inference_epoch,
            subscription_epoch=subscription_epoch,
            warmup_end_epoch=warmup_end_epoch,
            first_monitor_epoch=first_monitor_epoch,
            subscription_to_first_visible_inference_sec=subscription_to_first_visible_inference_sec,
            startup_warmup_sec=startup_warmup,
            prediction_visibility_lag_sec=prediction_visibility_lag_sec,
            component_offsets_sec=component_offsets_sec,
        )

    def preload_history(self, slots: list[SlotObservation]) -> None:
        if not slots:
            return
        self.log_progress(
            f"warmstart preload begin slots={len(slots)} "
            f"range={slots[0].slot_start.isoformat()}..{slots[-1].slot_end.isoformat()}"
        )
        for slot in slots:
            self.record_slot(slot)
            history = self.history.setdefault(slot.group_id, deque(maxlen=max(self.current_model.input_window, 1)))
            history.append(slot.feature_row)
        self.log_progress(f"warmstart preload complete slots={len(slots)} groups={len({slot.group_id for slot in slots})}")

    def clone_model_version(self, version_key: str, source: str) -> ModelVersion:
        return ModelVersion(
            key=version_key,
            bundle_dir=self.current_model.bundle_dir,
            bundle_model_path=self.current_model.bundle_model_path,
            bundle_scaler_path=self.current_model.bundle_scaler_path,
            model_id=str(uuid.uuid4()),
            model=self.current_model.model,
            scaler=self.current_model.scaler,
            input_window=self.current_model.input_window,
            feature_order=list(self.current_model.feature_order),
            output_fields=list(self.current_model.output_fields),
            source=source,
            daisy_tid=self.current_model.daisy_tid,
            daisy_local_model_path=self.current_model.daisy_local_model_path,
            daisy_local_scaler_path=self.current_model.daisy_local_scaler_path,
        )

    def load_bundle(self, bundle_dir: Path, version_key: str, source: str) -> ModelVersion:
        config_path = bundle_dir / "config.json"
        with config_path.open("r", encoding="utf-8") as handle:
            bundle_cfg = json.load(handle)

        model_cfg = bundle_cfg.get("model", {})
        infer_cfg = bundle_cfg.get("inference", {})
        input_window = parse_int(infer_cfg.get("seq_length"), 30) or 30
        feature_order = list(infer_cfg.get("feature_order") or DEFAULT_FEATURE_ORDER)
        output_fields = list(infer_cfg.get("output_fields") or DEFAULT_OUTPUT_FIELDS)

        model_script = bundle_cfg.get("MODEL_SCRIPT", "model.py")
        model_class = self.resolve_model_class(bundle_dir / model_script)
        model_path = bundle_dir / bundle_cfg["MODEL_PATH"]
        scaler_path = bundle_dir / bundle_cfg["SCALER_PATH"]
        scaler = joblib.load(scaler_path)

        model = model_class(
            input_size=parse_int(model_cfg.get("input_size"), 10) or 10,
            output_size=parse_int(model_cfg.get("output_size"), 2) or 2,
            num_channels=model_cfg.get("num_channels") or [32, 64, 64, 64],
            kernel_size=parse_int(model_cfg.get("kernel_size"), 2) or 2,
            dropout=parse_float(model_cfg.get("dropout"), 0.2) or 0.2,
        )
        weights = np.load(model_path, allow_pickle=True)
        state = {}
        for key, value in zip(model.state_dict().keys(), weights, strict=True):
            state[key] = torch.tensor(value)
        model.load_state_dict(state, strict=True)
        model.eval()
        model = model.to(self.device)
        return ModelVersion(
            key=version_key,
            bundle_dir=bundle_dir,
            bundle_model_path=model_path,
            bundle_scaler_path=scaler_path,
            model_id=str(uuid.uuid4()),
            model=model,
            scaler=scaler,
            input_window=input_window,
            feature_order=feature_order,
            output_fields=output_fields,
            source=source,
        )

    def resolve_seed_model_path(self) -> Path | None:
        if not self.enable_continue_learning:
            return None
        if self.current_model.daisy_local_model_path is not None:
            return self.current_model.daisy_local_model_path
        return self.current_model.bundle_model_path

    def resolve_seed_scaler_path(self) -> Path | None:
        if not self.use_fixed_scaler:
            return None
        return self.fixed_scaler_seed_path

    def resolve_model_class(self, model_script: Path) -> Any:
        if model_script.exists():
            spec = importlib.util.spec_from_file_location(f"bundle_model_{uuid.uuid4().hex}", model_script)
            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                return getattr(module, "Model", getattr(module, "TCNModel"))

        fallback = ROOT / ".agent" / "NWDAF-ML-Service" / "nwdaf_ml_service" / "ml" / "tcn.py"
        spec = importlib.util.spec_from_file_location("ml_service_tcn", fallback)
        if spec and spec.loader:
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            return getattr(module, "Model", getattr(module, "TCNModel"))
        raise RuntimeError(f"Unable to resolve model class from {model_script}")

    def sync_prediction_row(self, pred: PredictionRecord) -> None:
        row = self.prediction_rows_by_id.get(pred.prediction_id)
        if row is None:
            return
        row["baseTargetSimTime"] = pred.base_target_sim_time
        row["targetSlotSimTime"] = pred.target_slot_sim_time
        row["missCountFinal"] = pred.miss_count
        row["matchedActualUl"] = pred.matched_actual_ul
        row["matchedActualDl"] = pred.matched_actual_dl
        row["matchedActualSlotStart"] = pred.matched_actual_slot_start
        row["resolvedRoundIndex"] = pred.resolved_round_index
        row["resolvedAtSimTime"] = pred.resolved_at_sim_time
        row["discarded"] = pred.discarded
        row["discardReason"] = pred.discard_reason

    def derive_prediction_base_target_time(self, slot: SlotObservation) -> pd.Timestamp:
        if self.prediction_target_mode == "legacy_offset":
            return slot.slot_end + pd.Timedelta(seconds=self.prediction_target_offset_sec)
        return slot.slot_end

    def derive_prediction_visible_time(self, group_id: str, base_target_time: pd.Timestamp) -> pd.Timestamp:
        visibility_lag_sec = self.prediction_visibility_lag_sec_by_group.get(group_id, 0)
        return base_target_time + pd.Timedelta(seconds=visibility_lag_sec)

    def activate_scheduled_predictions(self, boundary: pd.Timestamp, *, inclusive: bool) -> int:
        activated = 0
        updated: list[PredictionRecord] = []
        for pred in self.scheduled_predictions:
            due = pred.predicted_at_sim_time <= boundary if inclusive else pred.predicted_at_sim_time < boundary
            if not due:
                updated.append(pred)
                continue
            self.prediction_store.add_prediction(pred)
            self.event_rows.append(
                {
                    "timestamp": pred.predicted_at_sim_time,
                    "eventType": "prediction_emitted",
                    "model": pred.model_version,
                    "scope": pred.scope,
                    "reason": None,
                    "detail": (
                        f"group={pred.group_id} baseTarget={pred.base_target_sim_time.isoformat()} "
                        f"target={pred.target_sim_time.isoformat()} "
                        f"targetSlot={pred.target_slot_sim_time.isoformat()} "
                        f"mode={self.prediction_target_mode} ul={pred.pred_ul} dl={pred.pred_dl}"
                    ),
                }
            )
            activated += 1
        self.scheduled_predictions = updated
        return activated

    def predict_next(self, group_id: str, slot: SlotObservation) -> None:
        history = self.history.setdefault(group_id, deque(maxlen=max(self.current_model.input_window, 1)))
        history.append(slot.feature_row)
        rows = list(history)
        if not rows:
            return
        frame = np.array([[row[name] for name in self.current_model.feature_order] for row in rows], dtype=np.float32)
        confidence = 0 if len(frame) < self.current_model.input_window else 80
        if len(frame) < self.current_model.input_window:
            pad = np.zeros((self.current_model.input_window - len(frame), frame.shape[1]), dtype=np.float32)
            frame = np.vstack([pad, frame])
        else:
            frame = frame[-self.current_model.input_window :]

        output_indices = [self.current_model.feature_order.index(name) for name in self.current_model.output_fields]
        transformed = np.log1p(frame)
        transformed = self.current_model.scaler.transform(transformed)
        tensor = torch.tensor(transformed.T, dtype=torch.float32).unsqueeze(0).to(self.device)
        with torch.no_grad():
            output = self.current_model.model(tensor)[0].cpu().numpy()
        unscaled = output * self.current_model.scaler.scale_[output_indices] + self.current_model.scaler.mean_[output_indices]
        ul_pred = int(max(0, np.expm1(unscaled[self.current_model.output_fields.index("ul_vol")])))
        dl_pred = int(max(0, np.expm1(unscaled[self.current_model.output_fields.index("dl_vol")])))

        base_target_time = self.derive_prediction_base_target_time(slot)
        predicted_at = self.derive_prediction_visible_time(group_id, base_target_time)
        target_time = base_target_time
        target_slot_time = target_time
        matured_at = predicted_at + pd.Timedelta(seconds=self.maturity_lag_sec)
        record = PredictionRecord(
            prediction_id=str(uuid.uuid4()),
            group_id=group_id,
            scope=f"group:{group_id}",
            model_version=self.current_model.key,
            model_source=str(self.current_model.bundle_dir),
            predicted_at_sim_time=predicted_at,
            base_target_sim_time=base_target_time,
            target_sim_time=target_time,
            target_slot_sim_time=target_slot_time,
            matured_at_sim_time=matured_at,
            pred_ul=ul_pred,
            pred_dl=dl_pred,
            confidence=confidence,
        )
        self.scheduled_predictions.append(record)
        row = {
            "predictionId": record.prediction_id,
            "predictedAtSimTime": predicted_at,
            "baseTargetSimTime": base_target_time,
            "targetSimTime": target_time,
            "targetSlotSimTime": target_slot_time,
            "groupId": group_id,
            "scope": record.scope,
            "modelVersion": record.model_version,
            "modelSource": record.model_source,
            "targetMode": self.prediction_target_mode,
            "predUl": ul_pred,
            "predDl": dl_pred,
            "confidence": confidence,
            "matchedActualUl": None,
            "matchedActualDl": None,
            "matchedActualSlotStart": None,
            "maturedAtSimTime": matured_at,
            "missCountFinal": 0,
            "resolvedRoundIndex": None,
            "resolvedAtSimTime": None,
            "discarded": False,
            "discardReason": None,
        }
        self.prediction_rows.append(row)
        self.prediction_rows_by_id[record.prediction_id] = row
        self.inference_since_last_monitor[group_id] = self.inference_since_last_monitor.get(group_id, 0) + 1

    def record_slot(self, slot: SlotObservation) -> None:
        self.actual_slots_by_group_time[(slot.group_id, slot.slot_start)] = slot
        self.slot_observations.append(slot)
        self.slots_rows.append(
            {
                "simTime": slot.slot_start,
                "windowIndex": slot.window_index,
                "groupId": slot.group_id,
                "slotStart": slot.slot_start,
                "slotEnd": slot.slot_end,
                "actualUl": slot.ul_vol,
                "actualDl": slot.dl_vol,
                "actualTotal": slot.ul_vol + slot.dl_vol,
                "actualUlPkts": slot.ul_pkts,
                "actualDlPkts": slot.dl_pkts,
                "actualUlThr": slot.ul_thr,
                "actualDlThr": slot.dl_thr,
                "actualUlPktThr": slot.ul_pkt_thr,
                "actualDlPktThr": slot.dl_pkt_thr,
                "notificationDocId": slot.notification_doc["correlationId"],
                "notificationDoc": json.dumps(slot.notification_doc),
            }
        )

    def maybe_activate_model(self, sim_time: pd.Timestamp) -> None:
        if self.pending_activation is None:
            return
        if sim_time < self.pending_activation.effective_sim_time:
            return
        self.current_model = self.pending_activation.model_version
        self.monitor_state = MonitorStateStore()
        discarded_predictions = self.prediction_store.discard_predictions_before(
            sim_time,
            discard_reason="model_swap_pending_expired",
        )
        for pred in discarded_predictions:
            self.sync_prediction_row(pred)
        self.retraining_until = None
        job = self.pending_activation.retrain_job
        job["status"] = "activated"
        job["modelVersionAfter"] = self.current_model.key
        self.event_rows.append(
            {
                "timestamp": sim_time,
                "eventType": "hot_swap_complete",
                "model": self.current_model.key,
                "scope": job["scope"],
                "reason": job["reason"],
                "detail": (
                    f"model swap activated: {self.current_model.key} "
                    f"discarded_pending_predictions={len(discarded_predictions)}"
                ),
            }
        )
        self.log_progress(
            f"hot swap complete model={self.current_model.key} sim_time={sim_time.isoformat()} "
            f"scope={job['scope']} reason={job['reason']}"
        )
        self.pending_activation = None

    def lookup_ground_truth_slot(self, group_id: str, target_slot_time: pd.Timestamp) -> SlotObservation | None:
        return self.actual_slots_by_group_time.get((group_id, target_slot_time))

    def delay_for_group(self, group_id: str) -> int:
        return self.subscription_to_first_urr_delay_sec_by_group.get(
            group_id, self.subscription_to_first_urr_delay_sec
        )

    def prediction_max_miss_count(self) -> int:
        sampling_interval = max(self.sampling_interval, 1)
        check_interval = max(self.check_interval_sec, 1)
        return max(1, int(math.ceil((2 * sampling_interval) / check_interval)) + 1)

    def discard_warmup_predictions(self, group_id: str, warmup_end: pd.Timestamp) -> int:
        discarded_predictions = self.prediction_store.discard_all_predictions(
            group_id=group_id,
            discard_reason="warmup",
            round_time=warmup_end,
        )
        discarded = len(discarded_predictions)
        for pred in discarded_predictions:
            self.sync_prediction_row(pred)
        self.pending_warmup_discard_counts[group_id] = discarded
        self.pending_warmup_discard_applied_groups.add(group_id)
        self.event_rows.append(
            {
                "timestamp": warmup_end,
                "eventType": "monitor_warmup_discard",
                "model": self.current_model.key,
                "scope": f"group:{group_id}",
                "reason": None,
                "detail": f"discarded_predictions={discarded}",
            }
        )
        self.log_progress(
            f"monitor warmup discard complete group={group_id} sim_time={warmup_end.isoformat()} "
            f"discarded_predictions={discarded}"
        )
        self.inference_since_last_monitor[group_id] = 0
        self.startup_warmup_done_groups.add(group_id)
        return discarded

    def run_monitor_round(self, group_id: str, round_time: pd.Timestamp) -> None:
        self.monitor_round_count += 1
        round_index = self.monitor_round_count
        scope = f"group:{group_id}"
        pending = self.prediction_store.snapshot_predictions(group_id)
        max_miss_count = self.prediction_max_miss_count()
        matched_predictions: list[PredictionRecord] = []
        matched_ids: set[str] = set()
        missed_ids: set[str] = set()
        for pred in pending:
            actual = self.lookup_ground_truth_slot(pred.group_id, pred.target_slot_sim_time)
            if actual is None:
                missed_ids.add(pred.prediction_id)
                continue
            pred.matched_actual_ul = actual.ul_vol
            pred.matched_actual_dl = actual.dl_vol
            pred.matched_actual_slot_start = actual.slot_start
            pred.resolved_round_index = round_index
            pred.resolved_at_sim_time = round_time
            matched_ids.add(pred.prediction_id)
            matched_predictions.append(pred)

        matched_count, discarded_count, discarded_predictions = self.prediction_store.resolve_predictions(
            matched_ids,
            missed_ids,
            max_miss_count,
            round_index,
            round_time,
        )
        for pred in pending:
            self.sync_prediction_row(pred)

        pairs = [
            (pred.pred_ul, pred.pred_dl, pred.matched_actual_ul or 0, pred.matched_actual_dl or 0)
            for pred in matched_predictions
        ]
        metrics = compute_metrics(pairs)
        traffic_scale = mean_abs_actual(pairs)
        predicted_traffic_scale = mean_abs_pred(pairs)
        warmup_discard_applied = group_id in self.pending_warmup_discard_applied_groups
        warmup_discarded_predictions = self.pending_warmup_discard_counts.pop(group_id, 0)
        self.pending_warmup_discard_applied_groups.discard(group_id)
        report = {
            "simTime": round_time,
            "modelVersion": self.current_model.key,
            "scope": scope,
            "groupId": group_id,
            "sampleCount": len(pairs),
            "inferenceNum": self.inference_since_last_monitor.get(group_id, 0),
            "windowStart": min((pred.target_sim_time for pred in matched_predictions), default=None),
            "windowEnd": max((pred.target_sim_time for pred in matched_predictions), default=None),
            "metricsJson": json.dumps({metric: metrics[metric] for metric in self.metrics_to_record if metric in metrics}),
            "trafficScale": traffic_scale,
            "predictedTrafficScale": predicted_traffic_scale,
            "pendingSnapshotSize": len(pending),
            "matchedPredictionCount": matched_count,
            "missedPredictionCount": len(missed_ids),
            "discardedPredictionCount": discarded_count,
            "warmupDiscardApplied": warmup_discard_applied,
            "warmupDiscardedPredictions": warmup_discarded_predictions,
            "predictionMaxMissCount": max_miss_count,
        }
        self.monitor_rows.append(report)
        for pred in matched_predictions:
            self.event_rows.append(
                {
                    "timestamp": round_time,
                    "eventType": "prediction_resolved",
                    "model": self.current_model.key,
                    "scope": pred.scope,
                    "reason": None,
                    "detail": (
                        f"predictionId={pred.prediction_id} group={pred.group_id} "
                        f"targetSlot={pred.target_slot_sim_time.isoformat()} "
                        f"actualSlot={pred.matched_actual_slot_start.isoformat() if pred.matched_actual_slot_start is not None else 'none'}"
                    ),
                }
            )
        for pred in discarded_predictions:
            self.event_rows.append(
                {
                    "timestamp": round_time,
                    "eventType": "prediction_discarded",
                    "model": self.current_model.key,
                    "scope": pred.scope,
                    "reason": pred.discard_reason,
                    "detail": (
                        f"predictionId={pred.prediction_id} group={pred.group_id} "
                        f"targetSlot={pred.target_slot_sim_time.isoformat()} missCount={pred.miss_count}"
                    ),
                }
            )
        self.event_rows.append(
            {
                "timestamp": round_time,
                "eventType": "monitor_snapshot",
                "model": self.current_model.key,
                "scope": scope,
                "reason": None,
                "detail": (
                    f"pending={len(pending)} matched={matched_count} missed={len(missed_ids)} "
                    f"discarded={discarded_count} samples={len(pairs)} "
                    f"inferenceNum={self.inference_since_last_monitor.get(group_id, 0)}"
                ),
            }
        )
        if not (len(pairs) < self.min_samples or self.retraining_until is not None and round_time < self.retraining_until):
            self.evaluate_policy(
                round_time,
                scope,
                metrics,
                traffic_scale,
                predicted_traffic_scale,
                len(pairs),
            )
        if pairs:
            self.inference_since_last_monitor[group_id] = 0

    def evaluate_policy(
        self,
        round_time: pd.Timestamp,
        scope: str,
        metrics: dict[str, float],
        traffic_scale: float,
        predicted_traffic_scale: float,
        sample_count: int,
    ) -> None:
        current = metrics.get(self.primary_metric)
        if current is None:
            return
        state = self.monitor_state.get_or_create_scope(scope, self.buffer_size, self.window_size)
        recent_history_count = state.sample_count(self.primary_metric)
        recent_baseline_ready = recent_history_count >= self.min_buffer_samples
        degradation_history_count = state.degradation_sample_count(self.primary_metric)
        mean = state.degradation_mean(self.primary_metric)
        std = state.degradation_std(self.primary_metric)
        degradation_baseline_ready = degradation_history_count >= self.min_buffer_samples

        observation = PolicyObservation(
            timestamp=round_time,
            sample_count=sample_count,
            traffic_scale=traffic_scale,
            predicted_traffic_scale=predicted_traffic_scale,
            metrics=dict(metrics),
        )
        state.record_observation(observation)

        degradation_cfg = self.acc_cfg.get("degradationPolicy") or {}
        degradation_min_scale = parse_float(degradation_cfg.get("minDecisionTrafficScale"), 0.0) or 0.0
        degradation_traffic_eligible = traffic_scale >= degradation_min_scale
        degradation_eligible = current > self.fixed_floor and degradation_traffic_eligible
        zscore = 0.0
        degradation_signal = False
        if degradation_history_count > 0:
            zscore = (current - mean) / max(std, self.min_std)
            degradation_signal = zscore > self.z_threshold
        degradation_signal_state = "skipped" if not degradation_baseline_ready else str(degradation_signal).lower()

        degradation_hit = degradation_baseline_ready and degradation_eligible and degradation_signal
        if degradation_baseline_ready:
            if degradation_traffic_eligible:
                degradation_hits = state.record_degradation_outcome(degradation_hit)
            else:
                degradation_hits = state.degradation_window.true_count()
        else:
            state.reset_degradation_window()
            degradation_hits = 0

        chronic_enabled = bool(self.chronic_cfg.get("enabled", False))
        chronic_metric = self.chronic_cfg.get("metric") or "WAPE"
        chronic_aggregator = self.chronic_cfg.get("aggregator") or "percentile"
        chronic_percentile = parse_int(self.chronic_cfg.get("percentile"), 75) or 75
        chronic_threshold = parse_float(self.chronic_cfg.get("threshold"), 1.0) or 1.0
        chronic_min_scale = parse_float(self.chronic_cfg.get("minDecisionTrafficScale"), 1024.0) or 1024.0
        chronic_eligible = False
        chronic_signal = False
        chronic_value = 0.0
        recent_traffic_scale_mean = state.mean(TRAFFIC_SCALE_METRIC)
        if chronic_enabled:
            chronic_eligible = recent_traffic_scale_mean >= chronic_min_scale
            if state.sample_count(chronic_metric) > 0:
                if chronic_aggregator == "mean":
                    chronic_value = state.mean(chronic_metric)
                else:
                    chronic_value = state.percentile(chronic_metric, chronic_percentile)
            chronic_signal = chronic_value > chronic_threshold
        chronic_signal_state = "skipped" if not recent_baseline_ready else str(chronic_signal).lower()
        chronic_hit = chronic_enabled and recent_baseline_ready and chronic_eligible and chronic_signal
        if chronic_enabled and recent_baseline_ready:
            chronic_hits = state.record_chronic_outcome(chronic_hit)
        else:
            if chronic_enabled:
                state.reset_chronic_window()
            chronic_hits = 0

        low_traffic_enabled = bool(self.low_traffic_cfg.get("enabled", False))
        low_traffic_max_actual = parse_float(self.low_traffic_cfg.get("maxActualTrafficScale"), 1024.0) or 1024.0
        low_traffic_min_predicted = parse_float(self.low_traffic_cfg.get("minPredictedTrafficScale"), 4096.0) or 4096.0
        low_traffic_ratio = parse_float(self.low_traffic_cfg.get("predictionOvershootRatio"), 4.0) or 4.0
        if low_traffic_ratio <= 1.0:
            low_traffic_ratio = 4.0
        low_traffic_eligible = False
        low_traffic_signal = False
        low_traffic_hit = False
        low_traffic_overshoot_ratio = 0.0
        if low_traffic_enabled:
            low_traffic_eligible = traffic_scale <= low_traffic_max_actual
            overshoot_base = max(traffic_scale, LOW_TRAFFIC_OVERSHOOT_EPSILON_BASE)
            low_traffic_overshoot_ratio = predicted_traffic_scale / overshoot_base
            low_traffic_signal = (
                predicted_traffic_scale >= low_traffic_min_predicted
                and predicted_traffic_scale >= low_traffic_ratio * overshoot_base
            )
        low_traffic_signal_state = "skipped" if not recent_baseline_ready else str(low_traffic_signal).lower()
        if low_traffic_enabled and recent_baseline_ready:
            low_traffic_hit = low_traffic_eligible and low_traffic_signal
            low_traffic_hits = state.record_low_traffic_outcome(low_traffic_hit)
        else:
            if low_traffic_enabled:
                state.reset_low_traffic_window()
            low_traffic_hits = 0

        baseline_not_full = degradation_history_count < self.min_buffer_samples
        should_record_degradation_reference = (
            sample_count >= self.min_samples
            and degradation_traffic_eligible
            and (baseline_not_full or not degradation_signal)
        )
        if should_record_degradation_reference:
            state.record_degradation_reference(observation)

        hit_reason = compose_hit_reason(degradation_hit, chronic_hit, low_traffic_hit)

        row = {
            "timestamp": round_time,
            "model": self.current_model.key,
            "scope": scope,
            "metric": self.primary_metric,
            "current": current,
            "mean": mean,
            "std": std,
            "zscore": zscore,
            "degradationEligible": degradation_eligible,
            "degradationSignal": degradation_signal_state,
            "baselineReady": degradation_baseline_ready,
            "degradationBaselineReady": degradation_baseline_ready,
            "recentBaselineReady": recent_baseline_ready,
            "actualTrafficScale": traffic_scale,
            "recentTrafficScaleMean": recent_traffic_scale_mean,
            "trafficScale": traffic_scale,
            "predictedTrafficScale": predicted_traffic_scale,
            "chronicEligible": chronic_eligible,
            "chronicSignal": chronic_signal_state,
            "chronicValue": chronic_value,
            "lowTrafficEligible": low_traffic_eligible,
            "lowTrafficSignal": low_traffic_signal_state,
            "lowTrafficOvershootRatio": low_traffic_overshoot_ratio,
            "degradationReferenceSamples": degradation_history_count,
            "recentSamples": recent_history_count,
            "degradationHits": degradation_hits,
            "degradationRequired": self.required_hits,
            "chronicHits": chronic_hits,
            "chronicRequired": self.required_hits,
            "lowTrafficHits": low_traffic_hits,
            "lowTrafficRequired": self.required_hits,
            "hitReason": hit_reason,
        }
        self.policy_rows.append(row)
        self.event_rows.append(
            {
                "timestamp": round_time,
                "eventType": "policy_evaluated",
                "model": self.current_model.key,
                "scope": scope,
                "reason": hit_reason,
                "detail": (
                    f"current={current:.4f} zscore={zscore:.4f} predictedTrafficScale={predicted_traffic_scale:.4f} "
                    f"lowTrafficOvershootRatio={low_traffic_overshoot_ratio:.4f} "
                    f"degradationHits={degradation_hits}/{self.required_hits} "
                    f"chronicHits={chronic_hits}/{self.required_hits} "
                    f"lowTrafficHits={low_traffic_hits}/{self.required_hits}"
                ),
            }
        )

        if (
            degradation_hits >= self.required_hits
            or chronic_hits >= self.required_hits
            or low_traffic_hits >= self.required_hits
        ):
            self.trigger_retrain(round_time, scope, hit_reason or "unknown")

    def select_training_docs(self, trigger_time: pd.Timestamp) -> dict[str, list[dict[str, Any]]]:
        start = trigger_time - pd.Timedelta(seconds=self.retrain_window_sec)
        selected: dict[str, list[dict[str, Any]]] = {}
        for slot in self.slot_observations:
            slot_start = slot.slot_start
            if start <= slot_start <= trigger_time:
                docs = slot.ue_notification_docs or [slot.notification_doc]
                selected.setdefault(str(slot.group_id), []).extend(json.loads(json.dumps(doc)) for doc in docs)
        return selected

    def trigger_retrain(self, trigger_time: pd.Timestamp, scope: str, reason: str) -> None:
        tid = uuid.uuid4().hex
        training_docs = self.select_training_docs(trigger_time)
        total_docs = sum(len(docs) for docs in training_docs.values())
        seed_model_path = self.resolve_seed_model_path()
        seed_scaler_path = self.resolve_seed_scaler_path()
        self.log_progress(
            f"retrain trigger tid={tid} scope={scope} reason={reason} sim_time={trigger_time.isoformat()} "
            f"groups={len(training_docs)} docs={total_docs} model={self.current_model.key} "
            f"fixed_scaler={self.use_fixed_scaler} continue_learning={self.enable_continue_learning}"
        )
        event = {
            "timestamp": trigger_time,
            "eventType": "retrain_trigger",
            "model": self.current_model.key,
            "scope": scope,
            "reason": reason,
            "detail": f"retrain triggered: tid={tid}",
        }
        self.event_rows.append(event)
        retrain_row = {
            "jobId": tid,
            "triggerSimTime": trigger_time,
            "reason": reason,
            "scope": scope,
            "modelVersionBefore": self.current_model.key,
            "trainingDataStart": trigger_time - pd.Timedelta(seconds=self.retrain_window_sec),
            "trainingDataEnd": trigger_time,
            "daisyTid": tid,
            "trainingWallStart": None,
            "trainingWallEnd": None,
            "trainingWallDurationSec": None,
            "swapEffectiveSimTime": None,
            "artifactLocation": None,
            "modelVersionAfter": None,
            "useFixedScaler": self.use_fixed_scaler,
            "enableContinueLearning": self.enable_continue_learning,
            "seedModelPath": str(seed_model_path) if seed_model_path is not None else None,
            "seedScalerPath": str(seed_scaler_path) if seed_scaler_path is not None else None,
            "status": "mock_pending" if self.skip_daisy else "triggered",
        }
        self.retrain_rows.append(retrain_row)
        if self.skip_daisy:
            wall_start = now_utc()
            wall_end = wall_start + pd.Timedelta(seconds=self.mock_training_duration_sec)
            effective_sim_time = trigger_time + pd.Timedelta(seconds=self.mock_training_duration_sec)
            model_version = self.clone_model_version(f"mock-{tid[:8]}", source="mock_skip_daisy")
            retrain_row.update(
                {
                    "trainingWallStart": wall_start,
                    "trainingWallEnd": wall_end,
                    "trainingWallDurationSec": float(self.mock_training_duration_sec),
                    "swapEffectiveSimTime": effective_sim_time,
                    "artifactLocation": str(self.current_model.bundle_dir),
                    "modelVersionAfter": model_version.key,
                    "status": "mock_trained",
                }
            )
            self.pending_activation = PendingActivation(
                effective_sim_time=effective_sim_time,
                model_version=model_version,
                retrain_job=retrain_row,
            )
            self.retraining_until = effective_sim_time
            self.monitor_state.reset_all()
            self.event_rows.append(
                {
                    "timestamp": trigger_time,
                    "eventType": "training_accepted",
                    "model": self.current_model.key,
                    "scope": scope,
                    "reason": reason,
                    "detail": f"mock training started: tid={tid}",
                }
            )
            self.event_rows.append(
                {
                    "timestamp": effective_sim_time,
                    "eventType": "training_complete",
                    "model": model_version.key,
                    "scope": scope,
                    "reason": reason,
                    "detail": f"mock training done: tid={tid} durationSec={self.mock_training_duration_sec}",
                }
            )
            self.log_progress(
                f"mock retrain scheduled tid={tid} effective_sim_time={effective_sim_time.isoformat()} "
                f"duration_sec={self.mock_training_duration_sec}"
            )
            return

        if not training_docs:
            retrain_row["status"] = "no_training_data"
            self.log_progress(f"retrain skipped tid={tid} reason=no_training_data")
            return

        self.daisy.ensure_started()
        wall_start = now_utc()
        self.log_progress(f"daisy training start tid={tid} wall_start={wall_start.isoformat()} groups={len(training_docs)} docs={total_docs}")
        self.event_rows.append(
            {
                "timestamp": trigger_time,
                "eventType": "training_accepted",
                "model": self.current_model.key,
                "scope": scope,
                "reason": reason,
                "detail": f"training started: tid={tid}",
            }
        )
        for group_id, docs in training_docs.items():
            self.daisy.upload_notifications(tid, group_id, docs, self.upload_batch_size)
        self.log_progress(
            f"daisy seed selection tid={tid} seed_model={seed_model_path} seed_scaler={seed_scaler_path}"
        )
        self.daisy.publish_task(
            tid,
            use_fixed_scaler=self.use_fixed_scaler,
            seed_model_path=seed_model_path,
            seed_scaler_path=seed_scaler_path,
        )
        self.log_progress(f"waiting daisy callback tid={tid}")
        callback_result = self.daisy.wait_for_result(tid)
        if callback_result.get("status") != "success":
            retrain_row["status"] = f"callback_{callback_result.get('status', 'unknown')}"
            raise RuntimeError(f"Daisy callback reported failure for {tid}: {callback_result}")
        artifact_dir = self.out_dir / "artifacts" / tid
        self.daisy.download_artifact(str(callback_result.get("model_url")), artifact_dir)
        wall_end = now_utc()
        duration_sec = max(1.0, (wall_end - wall_start).total_seconds())
        effective_sim_time = trigger_time + pd.Timedelta(seconds=duration_sec)
        model_version = self.load_bundle(artifact_dir, f"retrain-{tid[:8]}", source=f"daisy:{tid}")
        model_version.daisy_tid = tid
        model_version.daisy_local_model_path = self.daisy_example_dir / "model" / tid / "model.npy"
        model_version.daisy_local_scaler_path = self.daisy_example_dir / "model" / tid / "scaler.pkl"
        self.log_progress(
            f"daisy artifact ready tid={tid} artifact_dir={artifact_dir} "
            f"local_model={model_version.daisy_local_model_path} "
            f"local_scaler={model_version.daisy_local_scaler_path}"
        )
        retrain_row.update(
            {
                "trainingWallStart": wall_start,
                "trainingWallEnd": wall_end,
                "trainingWallDurationSec": duration_sec,
                "swapEffectiveSimTime": effective_sim_time,
                "artifactLocation": str(artifact_dir),
                "modelVersionAfter": model_version.key,
                "status": "trained",
            }
        )
        self.pending_activation = PendingActivation(effective_sim_time=effective_sim_time, model_version=model_version, retrain_job=retrain_row)
        self.retraining_until = effective_sim_time
        self.monitor_state.reset_all()
        self.log_progress(
            f"daisy training complete tid={tid} duration_sec={duration_sec:.2f} "
            f"effective_sim_time={effective_sim_time.isoformat()} model={model_version.key}"
        )
        self.event_rows.append(
            {
                "timestamp": effective_sim_time,
                "eventType": "training_complete",
                "model": model_version.key,
                "scope": scope,
                "reason": reason,
                "detail": f"training done: tid={tid} durationSec={duration_sec:.2f}",
            }
        )

    def write_outputs(self, manifest: dict[str, Any]) -> None:
        self.out_dir.mkdir(parents=True, exist_ok=True)
        (self.out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")
        (self.out_dir / "config.snapshot.yaml").write_text(
            yaml.safe_dump(
                {
                    "nwdafConfig": self.nw_cfg,
                    "replayConfig": self.replay_cfg,
                    "effective": manifest["effectiveConfig"],
                },
                sort_keys=False,
                allow_unicode=True,
            ),
            encoding="utf-8",
        )
        pd.DataFrame(self.slots_rows).to_parquet(self.out_dir / "slots.parquet", index=False)
        pd.DataFrame(self.prediction_rows).to_parquet(self.out_dir / "predictions.parquet", index=False)
        pd.DataFrame(self.monitor_rows).to_parquet(self.out_dir / "monitor_rounds.parquet", index=False)
        pd.DataFrame(self.policy_rows).to_parquet(self.out_dir / "policy.parquet", index=False)
        pd.DataFrame(self.retrain_rows).to_parquet(self.out_dir / "retrain_jobs.parquet", index=False)
        with (self.out_dir / "events.jsonl").open("w", encoding="utf-8") as handle:
            for row in self.event_rows:
                payload = {key: (value.isoformat() if isinstance(value, pd.Timestamp) else value) for key, value in row.items()}
                handle.write(json.dumps(payload, ensure_ascii=False) + "\n")

    def run(self, slots: list[SlotObservation], groups: list[str]) -> None:
        if not slots:
            raise RuntimeError("No slots loaded from dataset")
        self.total_live_slots = len(slots)
        self.processed_live_slots = 0
        self.next_progress_slot = self.progress_every_slots
        first_slot_by_group: dict[str, SlotObservation] = {}
        for slot in slots:
            first_slot_by_group.setdefault(slot.group_id, slot)

        startup_warmup_by_group: dict[str, int] = {}
        startup_profile_by_group: dict[str, StartupTimingProfile] = {}
        warmup_end_by_group: dict[str, pd.Timestamp] = {}
        warmup_discarded_by_group: dict[str, bool] = {}
        next_monitor_by_group: dict[str, pd.Timestamp] = {}
        for group_id in groups:
            first_slot = first_slot_by_group[group_id]
            startup_warmup = 0 if group_id in self.startup_warmup_done_groups else self.startup_warmup_sec
            startup_profile = self.build_startup_timing_profile(group_id, first_slot, startup_warmup)
            warmup_end = startup_profile.warmup_end_epoch
            next_monitor = startup_profile.first_monitor_epoch
            startup_warmup_by_group[group_id] = startup_warmup
            startup_profile_by_group[group_id] = startup_profile
            warmup_end_by_group[group_id] = warmup_end
            warmup_discarded_by_group[group_id] = startup_warmup == 0
            next_monitor_by_group[group_id] = next_monitor
        self.startup_profile_by_group = dict(startup_profile_by_group)
        self.prediction_visibility_lag_sec_by_group = {
            group_id: startup_profile_by_group[group_id].prediction_visibility_lag_sec for group_id in groups
        }
        self.log_progress(
            f"live replay begin slots={len(slots)} groups={len(groups)} "
            f"range={slots[0].slot_start.isoformat()}..{slots[-1].slot_end.isoformat()} "
            f"sampling={self.sampling_interval}s check_interval={self.check_interval_sec}s "
            f"group_schedules={json.dumps({group_id: {'mode': startup_profile_by_group[group_id].mode, 'warmupSec': startup_warmup_by_group[group_id], 'subscriptionToFirstVisibleInferenceSec': startup_profile_by_group[group_id].subscription_to_first_visible_inference_sec, 'predictionVisibilityLagSec': startup_profile_by_group[group_id].prediction_visibility_lag_sec, 'componentOffsetsSec': startup_profile_by_group[group_id].component_offsets_sec, 'subscriptionEpoch': startup_profile_by_group[group_id].subscription_epoch.isoformat(), 'firstUrrSignalEpoch': startup_profile_by_group[group_id].first_urr_signal_epoch.isoformat(), 'anchorEstablishedEpoch': startup_profile_by_group[group_id].anchor_established_epoch.isoformat(), 'firstDataSlotEpoch': startup_profile_by_group[group_id].first_data_slot_epoch.isoformat(), 'firstTargetSlotEpoch': startup_profile_by_group[group_id].first_target_slot_epoch.isoformat(), 'firstVisibleInferenceEpoch': startup_profile_by_group[group_id].first_visible_inference_epoch.isoformat(), 'warmupEnd': warmup_end_by_group[group_id].isoformat(), 'firstMonitor': next_monitor_by_group[group_id].isoformat()} for group_id in groups}, ensure_ascii=False)}"
        )
        batch: list[SlotObservation] = []
        current_batch_end: pd.Timestamp | None = None

        def advance_simulation(boundary: pd.Timestamp, *, inclusive: bool) -> None:
            def due(event_time: pd.Timestamp) -> bool:
                return event_time <= boundary if inclusive else event_time < boundary

            while True:
                next_event_time: pd.Timestamp | None = None
                if self.scheduled_predictions:
                    earliest_prediction_time = min(pred.predicted_at_sim_time for pred in self.scheduled_predictions)
                    if due(earliest_prediction_time):
                        next_event_time = earliest_prediction_time
                for scheduled_group in groups:
                    warmup_end = warmup_end_by_group[scheduled_group]
                    if not warmup_discarded_by_group[scheduled_group] and due(warmup_end):
                        if next_event_time is None or warmup_end < next_event_time:
                            next_event_time = warmup_end
                    next_monitor = next_monitor_by_group[scheduled_group]
                    if due(next_monitor):
                        if next_event_time is None or next_monitor < next_event_time:
                            next_event_time = next_monitor
                if next_event_time is None:
                    return

                self.activate_scheduled_predictions(next_event_time, inclusive=True)
                for scheduled_group in groups:
                    warmup_end = warmup_end_by_group[scheduled_group]
                    if not warmup_discarded_by_group[scheduled_group] and warmup_end == next_event_time:
                        self.discard_warmup_predictions(scheduled_group, warmup_end)
                        warmup_discarded_by_group[scheduled_group] = True
                for scheduled_group in groups:
                    while next_monitor_by_group[scheduled_group] == next_event_time:
                        self.run_monitor_round(scheduled_group, next_monitor_by_group[scheduled_group])
                        next_monitor_by_group[scheduled_group] += pd.Timedelta(seconds=self.check_interval_sec)

        def flush_batch(batch_slots: list[SlotObservation], batch_end: pd.Timestamp) -> None:
            if not batch_slots:
                return
            advance_simulation(batch_end, inclusive=False)
            for batch_slot in batch_slots:
                self.maybe_activate_model(batch_slot.slot_start)
                self.record_slot(batch_slot)
                self.predict_next(batch_slot.group_id, batch_slot)
            advance_simulation(batch_end, inclusive=True)

        for slot in slots:
            if current_batch_end is None:
                current_batch_end = slot.slot_end
            if slot.slot_end != current_batch_end:
                flush_batch(batch, current_batch_end)
                batch = []
                current_batch_end = slot.slot_end
            batch.append(slot)
            self.processed_live_slots += 1
            if (
                self.processed_live_slots == 1
                or self.processed_live_slots >= self.next_progress_slot
                or self.processed_live_slots == self.total_live_slots
            ):
                self.log_progress(
                    f"progress slots={self.processed_live_slots}/{self.total_live_slots} "
                    f"sim_time={slot.slot_end.isoformat()} model={self.current_model.key} "
                    f"monitor_rounds={len(self.monitor_rows)} policy_rows={len(self.policy_rows)} "
                    f"retrains={len(self.retrain_rows)}"
                )
                while self.processed_live_slots >= self.next_progress_slot:
                    self.next_progress_slot += self.progress_every_slots

        if current_batch_end is not None:
            flush_batch(batch, current_batch_end)

        manifest = {
            "kind": "retrain_replay_trace",
            "createdAt": now_utc().isoformat(),
            "datasetRoot": str(self.dataset_root.resolve()),
            "groups": groups,
            "effectiveConfig": {
                "samplingIntervalSec": self.sampling_interval,
                "reportPeriodSec": self.report_period_sec,
                "checkIntervalSec": self.check_interval_sec,
                "maturityLagSec": self.maturity_lag_sec,
                "startupTimingMode": self.startup_timing_mode,
                "startupTiming": self.startup_timing_cfg,
                "predictionTargetMode": self.prediction_target_mode,
                "predictionTargetOffsetSec": self.prediction_target_offset_sec,
                "subscriptionToFirstUrrDelaySec": self.subscription_to_first_urr_delay_sec,
                "subscriptionToFirstUrrDelaySecByGroup": self.subscription_to_first_urr_delay_sec_by_group,
                "retrainWindowSec": self.retrain_window_sec,
                "mockTrainingDurationSec": self.mock_training_duration_sec,
                "primaryMetric": self.primary_metric,
                "decisionWindowSize": self.window_size,
                "requiredHitsInWindow": self.required_hits,
                "pseudoWarmstartEnabled": bool(config_value(self.replay_cfg, "dataset.use_pseudo_warmstart", True)),
            },
            "summary": {
                "slots": len(self.slots_rows),
                "predictions": len(self.prediction_rows),
                "monitorRounds": len(self.monitor_rows),
                "policyRows": len(self.policy_rows),
                "retrainJobs": len(self.retrain_rows),
            },
        }
        self.write_outputs(manifest)
        self.daisy.stop()
        self.log_progress(
            f"replay complete slots={len(self.slots_rows)} predictions={len(self.prediction_rows)} "
            f"monitor_rounds={len(self.monitor_rows)} policy_rows={len(self.policy_rows)} "
            f"retrain_jobs={len(self.retrain_rows)}"
        )


def load_replay_config(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def load_nwdaf_config(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def read_group_breaking_time(group_dir: Path, override_sec: float | None) -> float:
    if override_sec is not None and override_sec > 0:
        return float(override_sec)
    meta_path = group_dir / "file.json"
    if not meta_path.exists():
        return 300.0
    try:
        payload = json.loads(meta_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return 300.0
    value = parse_float(payload.get("breaking time"), 300.0)
    return value if value and value > 0 else 300.0


def build_ue_notification_doc(
    group_id: str,
    ue_ip: str,
    slot_start: pd.Timestamp,
    slot_end: pd.Timestamp,
    ul_vol: int,
    dl_vol: int,
    ul_pkts: int,
    dl_pkts: int,
    ul_thr: float,
    dl_thr: float,
    ul_pkt_thr: float,
    dl_pkt_thr: float,
    seq: int,
) -> dict[str, Any]:
    return {
        "notificationItems": [
            {
                "eventType": "USER_DATA_USAGE_MEASURES",
                "timeStamp": isoformat_utc(slot_end),
                "ueIpv4Addr": ue_ip,
                "startTime": isoformat_utc(slot_start),
                "userDataUsageMeasurements": [
                    {
                        "volumeMeasurement": {
                            "totalVolume": ul_vol + dl_vol,
                            "ulVolume": ul_vol,
                            "dlVolume": dl_vol,
                            "totalNbOfPackets": ul_pkts + dl_pkts,
                            "ulNbOfPackets": ul_pkts,
                            "dlNbOfPackets": dl_pkts,
                        },
                        "throughputMeasurement": {
                            "ulThroughput": format_bps(ul_thr),
                            "dlThroughput": format_bps(dl_thr),
                            "ulPacketThroughput": format_pps(ul_pkt_thr),
                            "dlPacketThroughput": format_pps(dl_pkt_thr),
                        },
                    }
                ],
            }
        ],
        "correlationId": f"replay_{group_id}_{ue_ip}_{seq:06d}",
    }


def build_group_notification_doc(
    group_id: str,
    slot_start: pd.Timestamp,
    slot_end: pd.Timestamp,
    ul_vol: int,
    dl_vol: int,
    ul_pkts: int,
    dl_pkts: int,
    ul_thr: float,
    dl_thr: float,
    ul_pkt_thr: float,
    dl_pkt_thr: float,
    seq: int,
) -> dict[str, Any]:
    return {
        "notificationItems": [
            {
                "eventType": "USER_DATA_USAGE_MEASURES",
                "timeStamp": isoformat_utc(slot_end),
                "ueIpv4Addr": "0.0.0.0",
                "startTime": isoformat_utc(slot_start),
                "userDataUsageMeasurements": [
                    {
                        "volumeMeasurement": {
                            "totalVolume": ul_vol + dl_vol,
                            "ulVolume": ul_vol,
                            "dlVolume": dl_vol,
                            "totalNbOfPackets": ul_pkts + dl_pkts,
                            "ulNbOfPackets": ul_pkts,
                            "dlNbOfPackets": dl_pkts,
                        },
                        "throughputMeasurement": {
                            "ulThroughput": format_bps(ul_thr),
                            "dlThroughput": format_bps(dl_thr),
                            "ulPacketThroughput": format_pps(ul_pkt_thr),
                            "dlPacketThroughput": format_pps(dl_pkt_thr),
                        },
                    }
                ],
            }
        ],
        "correlationId": f"replay_{group_id}_{seq:06d}",
    }


def aggregate_ue_windows(group_id: str, ue_windows: list[UeWindowObservation], report_period_sec: int) -> list[SlotObservation]:
    grouped: dict[int, list[UeWindowObservation]] = {}
    for window in ue_windows:
        grouped.setdefault(window.window_index, []).append(window)

    slots: list[SlotObservation] = []
    seq = 0
    for window_index in sorted(grouped):
        members = grouped[window_index]
        slot_start = members[0].slot_start
        slot_end = members[0].slot_end
        ul_vol = sum(item.ul_vol for item in members)
        dl_vol = sum(item.dl_vol for item in members)
        ul_pkts = sum(item.ul_pkts for item in members)
        dl_pkts = sum(item.dl_pkts for item in members)
        ul_thr = (ul_vol * 8) / report_period_sec
        dl_thr = (dl_vol * 8) / report_period_sec
        ul_pkt_thr = ul_pkts / report_period_sec
        dl_pkt_thr = dl_pkts / report_period_sec
        slots.append(
            SlotObservation(
                group_id=group_id,
                window_index=window_index,
                slot_start=slot_start,
                slot_end=slot_end,
                ul_vol=ul_vol,
                dl_vol=dl_vol,
                ul_pkts=ul_pkts,
                dl_pkts=dl_pkts,
                ul_thr=ul_thr,
                dl_thr=dl_thr,
                ul_pkt_thr=ul_pkt_thr,
                dl_pkt_thr=dl_pkt_thr,
                notification_doc=build_group_notification_doc(
                    group_id,
                    slot_start,
                    slot_end,
                    ul_vol,
                    dl_vol,
                    ul_pkts,
                    dl_pkts,
                    ul_thr,
                    dl_thr,
                    ul_pkt_thr,
                    dl_pkt_thr,
                    seq,
                ),
                ue_notification_docs=[item.notification_doc for item in members],
            )
        )
        seq += 1
    return slots


def load_group_slots(
    group_dir: Path,
    report_period_sec: int,
    start_offset_sec: int = 0,
    max_slots: int | None = None,
    use_pseudo_warmstart: bool = True,
    breaking_time_override_sec: float | None = None,
) -> GroupReplayData:
    ue_windows: list[UeWindowObservation] = []
    parquet_files = sorted(group_dir.glob("training_packets_run*.parquet"))
    ue_offsets: dict[str, float] = {}
    ue_seq: dict[str, int] = {}
    for parquet_path in parquet_files:
        df = pd.read_parquet(parquet_path, columns=["ts", "direction", "len", "action", "ue_ip"])
        if df.empty:
            continue
        df = df.rename(columns={"ts": "timestamp", "len": "length"})
        df["dir_norm"] = df["direction"].map(parse_direction)
        df = df[df["dir_norm"].isin(["ul", "dl"])].copy()
        df = df[df["timestamp"] >= 0].copy()
        if df.empty:
            continue
        for ue_ip, ue_df in df.groupby("ue_ip", sort=True):
            ue_df = ue_df.copy()
            min_ts = float(ue_df["timestamp"].min())
            max_ts = float(ue_df["timestamp"].max())
            offset = ue_offsets.get(str(ue_ip), 0.0)
            ue_df["global_ts"] = (ue_df["timestamp"] - min_ts) + offset
            if start_offset_sec > 0:
                ue_df = ue_df[ue_df["global_ts"] >= start_offset_sec].copy()
            if not ue_df.empty:
                ue_df["slot_index"] = np.floor(ue_df["global_ts"] / report_period_sec).astype(int)
                grouped = (
                    ue_df.groupby("slot_index", sort=True)
                    .agg(
                        ul_vol=("length", lambda s: int(s[ue_df.loc[s.index, "dir_norm"] == "ul"].sum())),
                        dl_vol=("length", lambda s: int(s[ue_df.loc[s.index, "dir_norm"] == "dl"].sum())),
                        ul_pkts=("dir_norm", lambda s: int((s == "ul").sum())),
                        dl_pkts=("dir_norm", lambda s: int((s == "dl").sum())),
                    )
                    .reset_index()
                )
                max_slot_index = int(grouped["slot_index"].max()) if not grouped.empty else -1
                by_slot = grouped.set_index("slot_index").to_dict("index")
                seq = ue_seq.get(str(ue_ip), 0)
                for slot_index in range(max_slot_index + 1):
                    row = by_slot.get(slot_index)
                    ul_vol = int(row["ul_vol"]) if row else 0
                    dl_vol = int(row["dl_vol"]) if row else 0
                    ul_pkts = int(row["ul_pkts"]) if row else 0
                    dl_pkts = int(row["dl_pkts"]) if row else 0
                    slot_start = pd.to_datetime(slot_index * report_period_sec, unit="s", utc=True)
                    slot_end = slot_start + pd.Timedelta(seconds=report_period_sec)
                    ul_thr = (ul_vol * 8) / report_period_sec
                    dl_thr = (dl_vol * 8) / report_period_sec
                    ul_pkt_thr = ul_pkts / report_period_sec
                    dl_pkt_thr = dl_pkts / report_period_sec
                    ue_windows.append(
                        UeWindowObservation(
                            group_id=group_dir.name,
                            ue_ip=str(ue_ip),
                            window_index=int(slot_index),
                            slot_start=slot_start,
                            slot_end=slot_end,
                            ul_vol=ul_vol,
                            dl_vol=dl_vol,
                            ul_pkts=ul_pkts,
                            dl_pkts=dl_pkts,
                            ul_thr=ul_thr,
                            dl_thr=dl_thr,
                            ul_pkt_thr=ul_pkt_thr,
                            dl_pkt_thr=dl_pkt_thr,
                            notification_doc=build_ue_notification_doc(
                                group_dir.name,
                                str(ue_ip),
                                slot_start,
                                slot_end,
                                ul_vol,
                                dl_vol,
                                ul_pkts,
                                dl_pkts,
                                ul_thr,
                                dl_thr,
                                ul_pkt_thr,
                                dl_pkt_thr,
                                seq,
                            ),
                        )
                    )
                    seq += 1
                ue_seq[str(ue_ip)] = seq
            ue_offsets[str(ue_ip)] = offset + max(0.0, (max_ts - min_ts)) + 0.001
    breaking_time_sec = read_group_breaking_time(group_dir, breaking_time_override_sec)
    aligned_breaking = int(math.ceil(breaking_time_sec / report_period_sec) * report_period_sec) if use_pseudo_warmstart else 0
    slots = aggregate_ue_windows(group_dir.name, ue_windows, report_period_sec)
    if max_slots is not None:
        slots = slots[:max_slots]
        max_slot_end = slots[-1].slot_end if slots else pd.Timestamp(0, unit="s", tz="UTC")
        ue_windows = [window for window in ue_windows if window.slot_end <= max_slot_end]
    return GroupReplayData(
        group_id=group_dir.name,
        slots=slots,
        ue_windows=ue_windows,
        breaking_time_sec=breaking_time_sec,
        aligned_breaking_time_sec=aligned_breaking,
    )


def run_command(args: argparse.Namespace) -> None:
    nw_cfg = load_nwdaf_config(args.config)
    replay_cfg = load_replay_config(args.replay_config)
    groups = args.groups or config_value(replay_cfg, "dataset.groups", []) or [path.name for path in sorted(args.dataset_root.iterdir()) if path.is_dir()]
    report_period_sec = parse_int(config_value(replay_cfg, "dataset.report_period_sec"), 5) or 5
    start_offset_sec = parse_int(config_value(replay_cfg, "dataset.start_offset_sec"), 0) or 0
    max_slots = parse_int(config_value(replay_cfg, "dataset.max_slots"), None)
    use_pseudo_warmstart = bool(config_value(replay_cfg, "dataset.use_pseudo_warmstart", True))
    breaking_time_override_sec = parse_float(config_value(replay_cfg, "dataset.breaking_time_sec"), None)
    console_log(
        f"run start dataset_root={args.dataset_root.resolve()} groups={groups} "
        f"config={args.config.resolve()} replay_config={args.replay_config.resolve()} "
        f"skip_daisy={args.skip_daisy}"
    )

    preload_slots: list[SlotObservation] = []
    live_slots: list[SlotObservation] = []
    for group in groups:
        group_data = load_group_slots(
            args.dataset_root / group,
            report_period_sec,
            start_offset_sec,
            max_slots,
            use_pseudo_warmstart=use_pseudo_warmstart,
            breaking_time_override_sec=breaking_time_override_sec,
        )
        if use_pseudo_warmstart and group_data.aligned_breaking_time_sec > 0:
            split_window_index = max(0, (group_data.aligned_breaking_time_sec // report_period_sec) - 1)
            preload_slots.extend([slot for slot in group_data.slots if slot.window_index < split_window_index])
            live_slots.extend([slot for slot in group_data.slots if slot.window_index >= split_window_index])
        else:
            live_slots.extend(group_data.slots)
    preload_slots.sort(key=lambda slot: (slot.slot_start, slot.group_id))
    live_slots.sort(key=lambda slot: (slot.slot_start, slot.group_id))
    console_log(
        f"dataset prepared preload_slots={len(preload_slots)} live_slots={len(live_slots)} "
        f"warmstart={use_pseudo_warmstart}"
    )

    out_dir = args.out.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    engine = ReplayEngine(
        nw_cfg=nw_cfg,
        replay_cfg=replay_cfg,
        initial_bundle=args.initial_bundle.resolve(),
        daisy_example_dir=args.daisy_example_dir.resolve(),
        out_dir=out_dir,
        skip_daisy=args.skip_daisy,
        dataset_root=args.dataset_root.resolve(),
    )
    if preload_slots:
        engine.preload_history(preload_slots)
    engine.run(live_slots, groups)
    print(
        f"slots={len(engine.slots_rows)} predictions={len(engine.prediction_rows)} "
        f"monitor_rounds={len(engine.monitor_rows)} policy_rows={len(engine.policy_rows)} "
        f"retrain_jobs={len(engine.retrain_rows)}"
    )


def main() -> None:
    args = parse_args()
    if args.command == "run":
        run_command(args)


if __name__ == "__main__":
    main()
