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


def now_utc() -> pd.Timestamp:
    return pd.Timestamp.now(tz="UTC")


def console_log(message: str) -> None:
    ts = now_utc().strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[retrain-replay] {ts} {message}", flush=True)


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


class RingBuffer:
    def __init__(self, size: int):
        self.size = max(1, size)
        self.values: deque[float] = deque(maxlen=self.size)

    def add(self, value: float) -> None:
        self.values.append(float(value))

    def count(self) -> int:
        return len(self.values)

    def snapshot(self) -> list[float]:
        return list(self.values)


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
        self.metric_buffers: dict[str, RingBuffer] = {}
        self.degradation_window = HitWindow(decision_window_size)
        self.chronic_window = HitWindow(decision_window_size)
        self.buffer_size = buffer_size
        self.last_update: pd.Timestamp | None = None

    def record_metric(self, metric: str, value: float, now: pd.Timestamp) -> None:
        buffer = self.metric_buffers.get(metric)
        if buffer is None:
            buffer = RingBuffer(self.buffer_size)
            self.metric_buffers[metric] = buffer
        buffer.add(value)
        self.last_update = now

    def values(self, metric: str) -> list[float]:
        buffer = self.metric_buffers.get(metric)
        return buffer.snapshot() if buffer else []

    def sample_count(self, metric: str) -> int:
        buffer = self.metric_buffers.get(metric)
        return buffer.count() if buffer else 0

    def mean(self, metric: str) -> float:
        values = self.values(metric)
        return float(np.mean(values)) if values else 0.0

    def std(self, metric: str) -> float:
        values = self.values(metric)
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

    def reset_decision_windows(self) -> None:
        self.degradation_window.reset()
        self.chronic_window.reset()


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
    breaking_time_sec: float
    aligned_breaking_time_sec: int


@dataclass
class ModelVersion:
    key: str
    bundle_dir: Path
    model_id: str
    model: Any
    scaler: Any
    input_window: int
    feature_order: list[str]
    output_fields: list[str]
    source: str


@dataclass
class PredictionRecord:
    prediction_id: str
    group_id: str
    scope: str
    model_version: str
    model_source: str
    predicted_at_sim_time: pd.Timestamp
    target_sim_time: pd.Timestamp
    matured_at_sim_time: pd.Timestamp
    pred_ul: int
    pred_dl: int
    confidence: int
    matched_actual_ul: int | None = None
    matched_actual_dl: int | None = None
    consumed: bool = False


@dataclass
class PendingActivation:
    effective_sim_time: pd.Timestamp
    model_version: ModelVersion
    retrain_job: dict[str, Any]


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
            f"log={log_path}"
        )
        self.master_proc = subprocess.Popen(
            cmd,
            cwd=self.example_dir,
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

    def publish_task(self, tid: str) -> None:
        payload = json.loads(json.dumps(self.task_cfg))
        payload["TID"] = tid
        payload["CALLBACK_URL"] = self.callback_server.address
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
        self.chronic_cfg = self.acc_cfg.get("chronicPolicy") or {}

        mtlf_cfg = config_value(nw_cfg, "configuration.mtlf", {}) or {}
        self.task_cfg = mtlf_cfg.get("task") or {}
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
        self.pending_activation: PendingActivation | None = None
        self.retraining_until: pd.Timestamp | None = None
        self.history: dict[str, deque[dict[str, float]]] = {}
        self.actual_slots_by_group_time: dict[tuple[str, pd.Timestamp], SlotObservation] = {}
        self.predictions: list[PredictionRecord] = []
        self.monitor_state = MonitorStateStore()
        self.inference_since_last_monitor = 0

        self.slots_rows: list[dict[str, Any]] = []
        self.prediction_rows: list[dict[str, Any]] = []
        self.monitor_rows: list[dict[str, Any]] = []
        self.policy_rows: list[dict[str, Any]] = []
        self.retrain_rows: list[dict[str, Any]] = []
        self.event_rows: list[dict[str, Any]] = []
        self.processed_live_slots = 0
        self.total_live_slots = 0
        self.next_progress_slot = self.progress_every_slots
        self.monitor_round_count = 0

    def log_progress(self, message: str) -> None:
        console_log(message)

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
            model_id=str(uuid.uuid4()),
            model=self.current_model.model,
            scaler=self.current_model.scaler,
            input_window=self.current_model.input_window,
            feature_order=list(self.current_model.feature_order),
            output_fields=list(self.current_model.output_fields),
            source=source,
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
            model_id=str(uuid.uuid4()),
            model=model,
            scaler=scaler,
            input_window=input_window,
            feature_order=feature_order,
            output_fields=output_fields,
            source=source,
        )

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

        predicted_at = slot.slot_end
        target_time = slot.slot_end
        matured_at = predicted_at + pd.Timedelta(seconds=self.maturity_lag_sec)
        record = PredictionRecord(
            prediction_id=str(uuid.uuid4()),
            group_id=group_id,
            scope=f"group:{group_id}",
            model_version=self.current_model.key,
            model_source=str(self.current_model.bundle_dir),
            predicted_at_sim_time=predicted_at,
            target_sim_time=target_time,
            matured_at_sim_time=matured_at,
            pred_ul=ul_pred,
            pred_dl=dl_pred,
            confidence=confidence,
        )
        self.predictions.append(record)
        self.prediction_rows.append(
            {
                "predictionId": record.prediction_id,
                "predictedAtSimTime": predicted_at,
                "targetSimTime": target_time,
                "groupId": group_id,
                "scope": record.scope,
                "modelVersion": record.model_version,
                "modelSource": record.model_source,
                "predUl": ul_pred,
                "predDl": dl_pred,
                "confidence": confidence,
                "matchedActualUl": None,
                "matchedActualDl": None,
                "maturedAtSimTime": matured_at,
            }
        )
        self.event_rows.append(
            {
                "timestamp": predicted_at,
                "eventType": "prediction_emitted",
                "model": record.model_version,
                "scope": record.scope,
                "reason": None,
                "detail": f"group={group_id} target={target_time.isoformat()} ul={ul_pred} dl={dl_pred}",
            }
        )
        self.inference_since_last_monitor += 1

    def record_slot(self, slot: SlotObservation) -> None:
        self.actual_slots_by_group_time[(slot.group_id, slot.slot_start)] = slot
        self.slots_rows.append(
            {
                "simTime": slot.slot_start,
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
        self.predictions = [pred for pred in self.predictions if pred.target_sim_time >= sim_time]
        for pred in self.predictions:
            pred.consumed = False
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
                "detail": f"model swap activated: {self.current_model.key}",
            }
        )
        self.log_progress(
            f"hot swap complete model={self.current_model.key} sim_time={sim_time.isoformat()} "
            f"scope={job['scope']} reason={job['reason']}"
        )
        self.pending_activation = None

    def consume_mature_predictions(self, round_time: pd.Timestamp) -> list[PredictionRecord]:
        matured = []
        for pred in self.predictions:
            if pred.consumed or pred.matured_at_sim_time > round_time:
                continue
            actual = self.actual_slots_by_group_time.get((pred.group_id, pred.target_sim_time))
            if actual is None:
                continue
            pred.consumed = True
            pred.matched_actual_ul = actual.ul_vol
            pred.matched_actual_dl = actual.dl_vol
            matured.append(pred)
        return matured

    def run_monitor_round(self, round_time: pd.Timestamp) -> None:
        matured = self.consume_mature_predictions(round_time)
        if not matured:
            return
        self.monitor_round_count += 1
        total_pairs = len(matured)
        grouped: dict[str, list[PredictionRecord]] = {}
        for pred in matured:
            grouped.setdefault(pred.group_id, []).append(pred)

        for group_id, preds in grouped.items():
            scope = f"group:{group_id}"
            pairs = [(pred.pred_ul, pred.pred_dl, pred.matched_actual_ul or 0, pred.matched_actual_dl or 0) for pred in preds]
            metrics = compute_metrics(pairs)
            report = {
                "simTime": round_time,
                "modelVersion": self.current_model.key,
                "scope": scope,
                "groupId": group_id,
                "sampleCount": len(pairs),
                "inferenceNum": self.inference_since_last_monitor,
                "windowStart": min(pred.target_sim_time for pred in preds),
                "windowEnd": max(pred.target_sim_time for pred in preds),
                "metricsJson": json.dumps({metric: metrics[metric] for metric in self.metrics_to_record if metric in metrics}),
                "trafficScale": mean_abs_actual(pairs),
            }
            self.monitor_rows.append(report)
            self.event_rows.append(
                {
                    "timestamp": round_time,
                    "eventType": "monitor_round",
                    "model": self.current_model.key,
                    "scope": scope,
                    "reason": None,
                    "detail": f"samples={len(pairs)} inferenceNum={self.inference_since_last_monitor}",
                }
            )
            if total_pairs < self.min_samples or self.retraining_until is not None and round_time < self.retraining_until:
                continue
            self.evaluate_policy(round_time, scope, metrics, mean_abs_actual(pairs), len(pairs))
        self.inference_since_last_monitor = 0

    def evaluate_policy(self, round_time: pd.Timestamp, scope: str, metrics: dict[str, float], traffic_scale: float, sample_count: int) -> None:
        current = metrics.get(self.primary_metric)
        if current is None:
            return
        state = self.monitor_state.get_or_create_scope(scope, self.buffer_size, self.window_size)
        history_count = state.sample_count(self.primary_metric)
        mean = state.mean(self.primary_metric)
        std = state.std(self.primary_metric)
        baseline_ready = history_count >= self.min_buffer_samples

        for metric_name, value in metrics.items():
            state.record_metric(metric_name, value, round_time)
        state.record_metric(TRAFFIC_SCALE_METRIC, traffic_scale, round_time)

        degradation_eligible = current > self.fixed_floor
        zscore = 0.0
        degradation_signal = False
        if history_count > 0:
            zscore = (current - mean) / max(std, self.min_std)
            degradation_signal = zscore > self.z_threshold
        degradation_signal_state = "skipped" if not baseline_ready else str(degradation_signal).lower()

        degradation_hit = baseline_ready and degradation_eligible and degradation_signal
        if baseline_ready:
            degradation_hits = state.record_degradation_outcome(degradation_hit)
        else:
            state.reset_decision_windows()
            degradation_hits = 0

        chronic_enabled = bool(self.chronic_cfg.get("enabled", False))
        chronic_metric = self.chronic_cfg.get("metric") or "WAPE"
        chronic_aggregator = self.chronic_cfg.get("aggregator") or "percentile"
        chronic_percentile = parse_int(self.chronic_cfg.get("percentile"), 75) or 75
        chronic_threshold = parse_float(self.chronic_cfg.get("threshold"), 1.0) or 1.0
        chronic_min_scale = parse_float(self.chronic_cfg.get("minTrafficScale"), 1024.0) or 1024.0
        chronic_eligible = False
        chronic_signal = False
        chronic_value = 0.0
        if chronic_enabled:
            chronic_eligible = state.mean(TRAFFIC_SCALE_METRIC) >= chronic_min_scale
            if state.sample_count(chronic_metric) > 0:
                if chronic_aggregator == "mean":
                    chronic_value = state.mean(chronic_metric)
                else:
                    chronic_value = state.percentile(chronic_metric, chronic_percentile)
            chronic_signal = chronic_value > chronic_threshold
        chronic_signal_state = "skipped" if not baseline_ready else str(chronic_signal).lower()
        if chronic_enabled and baseline_ready:
            chronic_hits = state.record_chronic_outcome(chronic_eligible and chronic_signal)
        else:
            chronic_hits = 0

        hit_reason = "none"
        if degradation_hit and chronic_enabled and chronic_eligible and chronic_signal:
            hit_reason = "both"
        elif degradation_hit:
            hit_reason = "degradation"
        elif chronic_enabled and baseline_ready and chronic_eligible and chronic_signal:
            hit_reason = "chronic"

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
            "baselineReady": baseline_ready,
            "trafficScale": state.mean(TRAFFIC_SCALE_METRIC),
            "chronicEligible": chronic_eligible,
            "chronicSignal": chronic_signal_state,
            "chronicValue": chronic_value,
            "degradationHits": degradation_hits,
            "degradationRequired": self.required_hits,
            "chronicHits": chronic_hits,
            "chronicRequired": self.required_hits,
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
                "detail": f"current={current:.4f} zscore={zscore:.4f} degradationHits={degradation_hits}/{self.required_hits} chronicHits={chronic_hits}/{self.required_hits}",
            }
        )

        if degradation_hits >= self.required_hits or chronic_hits >= self.required_hits:
            self.trigger_retrain(round_time, scope, hit_reason or "unknown")

    def select_training_docs(self, trigger_time: pd.Timestamp) -> dict[str, list[dict[str, Any]]]:
        start = trigger_time - pd.Timedelta(seconds=self.retrain_window_sec)
        selected: dict[str, list[dict[str, Any]]] = {}
        for row in self.slots_rows:
            slot_start = row["slotStart"]
            if start <= slot_start <= trigger_time:
                selected.setdefault(str(row["groupId"]), []).append(json.loads(row["notificationDoc"]))
        return selected

    def trigger_retrain(self, trigger_time: pd.Timestamp, scope: str, reason: str) -> None:
        tid = uuid.uuid4().hex
        training_docs = self.select_training_docs(trigger_time)
        total_docs = sum(len(docs) for docs in training_docs.values())
        self.log_progress(
            f"retrain trigger tid={tid} scope={scope} reason={reason} sim_time={trigger_time.isoformat()} "
            f"groups={len(training_docs)} docs={total_docs} model={self.current_model.key}"
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
        self.daisy.publish_task(tid)
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
        next_monitor = slots[0].slot_start + pd.Timedelta(seconds=self.check_interval_sec)
        self.log_progress(
            f"live replay begin slots={len(slots)} groups={len(groups)} "
            f"range={slots[0].slot_start.isoformat()}..{slots[-1].slot_end.isoformat()} "
            f"sampling={self.sampling_interval}s check_interval={self.check_interval_sec}s"
        )
        for slot in slots:
            self.maybe_activate_model(slot.slot_start)
            self.record_slot(slot)
            self.predict_next(slot.group_id, slot)
            while next_monitor <= slot.slot_end:
                self.run_monitor_round(next_monitor)
                next_monitor += pd.Timedelta(seconds=self.check_interval_sec)
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


def build_notification_doc(group_id: str, slot_start: pd.Timestamp, slot_end: pd.Timestamp, ul_vol: int, dl_vol: int, ul_pkts: int, dl_pkts: int, ul_thr: float, dl_thr: float, ul_pkt_thr: float, dl_pkt_thr: float, seq: int) -> dict[str, Any]:
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


def load_group_slots(
    group_dir: Path,
    report_period_sec: int,
    start_offset_sec: int = 0,
    max_slots: int | None = None,
    use_pseudo_warmstart: bool = True,
    breaking_time_override_sec: float | None = None,
) -> GroupReplayData:
    slots: list[SlotObservation] = []
    parquet_files = sorted(group_dir.glob("training_packets_run*.parquet"))
    seq = 0
    global_time_offset = 0.0
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
        min_ts = float(df["timestamp"].min())
        max_ts = float(df["timestamp"].max())
        df["global_ts"] = (df["timestamp"] - min_ts) + global_time_offset
        if start_offset_sec > 0:
            df = df[df["global_ts"] >= start_offset_sec].copy()
        if df.empty:
            global_time_offset += max(0.0, (max_ts - min_ts)) + 0.001
            continue
        df["slot_index"] = np.floor(df["global_ts"] / report_period_sec).astype(int)
        grouped = (
            df.groupby("slot_index", sort=True)
            .agg(
                ul_vol=("length", lambda s: int(s[df.loc[s.index, "dir_norm"] == "ul"].sum())),
                dl_vol=("length", lambda s: int(s[df.loc[s.index, "dir_norm"] == "dl"].sum())),
                ul_pkts=("dir_norm", lambda s: int((s == "ul").sum())),
                dl_pkts=("dir_norm", lambda s: int((s == "dl").sum())),
            )
            .reset_index()
        )
        for _, row in grouped.iterrows():
            slot_start = pd.to_datetime(int(row["slot_index"]) * report_period_sec, unit="s", utc=True)
            slot_end = slot_start + pd.Timedelta(seconds=report_period_sec)
            ul_thr = (int(row["ul_vol"]) * 8) / report_period_sec
            dl_thr = (int(row["dl_vol"]) * 8) / report_period_sec
            ul_pkt_thr = int(row["ul_pkts"]) / report_period_sec
            dl_pkt_thr = int(row["dl_pkts"]) / report_period_sec
            doc = build_notification_doc(
                group_dir.name,
                slot_start,
                slot_end,
                int(row["ul_vol"]),
                int(row["dl_vol"]),
                int(row["ul_pkts"]),
                int(row["dl_pkts"]),
                ul_thr,
                dl_thr,
                ul_pkt_thr,
                dl_pkt_thr,
                seq,
            )
            slots.append(
                SlotObservation(
                    group_id=group_dir.name,
                    slot_start=slot_start,
                    slot_end=slot_end,
                    ul_vol=int(row["ul_vol"]),
                    dl_vol=int(row["dl_vol"]),
                    ul_pkts=int(row["ul_pkts"]),
                    dl_pkts=int(row["dl_pkts"]),
                    ul_thr=ul_thr,
                    dl_thr=dl_thr,
                    ul_pkt_thr=ul_pkt_thr,
                    dl_pkt_thr=dl_pkt_thr,
                    notification_doc=doc,
                )
            )
            seq += 1
            if max_slots is not None and len(slots) >= max_slots:
                breaking_time_sec = read_group_breaking_time(group_dir, breaking_time_override_sec)
                aligned_breaking = int(math.ceil(breaking_time_sec / report_period_sec) * report_period_sec) if use_pseudo_warmstart else 0
                return GroupReplayData(
                    group_id=group_dir.name,
                    slots=slots,
                    breaking_time_sec=breaking_time_sec,
                    aligned_breaking_time_sec=aligned_breaking,
                )
        global_time_offset += max(0.0, (max_ts - min_ts)) + 0.001
    breaking_time_sec = read_group_breaking_time(group_dir, breaking_time_override_sec)
    aligned_breaking = int(math.ceil(breaking_time_sec / report_period_sec) * report_period_sec) if use_pseudo_warmstart else 0
    return GroupReplayData(
        group_id=group_dir.name,
        slots=slots,
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
            split_time = pd.to_datetime(group_data.aligned_breaking_time_sec, unit="s", utc=True)
            preload_slots.extend([slot for slot in group_data.slots if slot.slot_start < split_time])
            live_slots.extend([slot for slot in group_data.slots if slot.slot_start >= split_time])
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
