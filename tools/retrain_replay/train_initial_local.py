#!/usr/bin/env python3
"""Train an initial NWDAF model bundle with centralized local supervision.

This script trains the same TCN bundle contract used by retrain_replay and
NWDAF-ML-Service, but without going through Daisy/FL.

Usage:
    uv run train_initial_local.py

Typical output:
    out/initial_local_cat1_30s/
      bundle/config.json
      bundle/model.npy
      bundle/model.py
      bundle/scaler.pkl
      meta.json
      train_metrics.json
      val_predictions.parquet
"""
from __future__ import annotations

import argparse
import copy
import json
import random
import shutil
import sys
import time
import uuid
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
import torch
from sklearn.preprocessing import StandardScaler
from torch import nn
from torch.utils.data import DataLoader, TensorDataset

REPLAY_DIR = Path(__file__).resolve().parent
NWDAF_ROOT = REPLAY_DIR.parents[1]
BASE_NWDAF_CFG = NWDAF_ROOT / "config" / "nwdafcfg.yaml"
DATASET_ROOT = REPLAY_DIR.parents[3] / "go-upf-ess" / "go-upf" / "pre_data"
ML_SERVICE_ROOT = NWDAF_ROOT.parent / "NWDAF-ML-Service"
TCN_SOURCE = ML_SERVICE_ROOT / "nwdaf_ml_service" / "ml" / "tcn.py"

sys.path.insert(0, str(REPLAY_DIR))
sys.path.insert(0, str(ML_SERVICE_ROOT))

from retrain_replay import config_value, load_group_slots, load_nwdaf_config, parse_int  # noqa: E402
from nwdaf_ml_service.ml.tcn import TCNModel  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dataset-root", type=Path, default=DATASET_ROOT)
    parser.add_argument("--config", type=Path, default=BASE_NWDAF_CFG)
    parser.add_argument("--out", type=Path, default=REPLAY_DIR / "out" / "initial_local_cat1_30s")
    parser.add_argument("--groups", nargs="*", default=["group1", "group2"])
    parser.add_argument("--cat-duration", type=int, default=1800, metavar="SEC",
                        help="Seconds of data to include from t=0 (default: 1800 = CAT1 only)")
    parser.add_argument("--period", type=int, default=30, metavar="SEC",
                        help="Slot aggregation period in seconds (default: 30)")
    parser.add_argument("--seq-length", type=int, default=30, metavar="N",
                        help="Number of historical slots in each training input window (default: 30)")
    parser.add_argument("--train-ratio", type=float, default=0.8,
                        help="Chronological train split ratio per group (default: 0.8)")
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument("--max-epochs", type=int, default=100)
    parser.add_argument("--learning-rate", type=float, default=1e-3)
    parser.add_argument("--weight-decay", type=float, default=0.0)
    parser.add_argument("--loss", choices=["huber", "mse", "mae"], default="huber")
    parser.add_argument("--huber-delta", type=float, default=1.0,
                        help="Delta parameter for Huber loss (default: 1.0)")
    parser.add_argument("--early-stopping-patience", type=int, default=10)
    parser.add_argument("--lr-patience", type=int, default=4)
    parser.add_argument("--min-delta", type=float, default=1e-4)
    parser.add_argument("--device", choices=["auto", "cpu", "cuda"], default="auto")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args()


def set_seed(seed: int) -> None:
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(seed)


def choose_device(requested: str) -> torch.device:
    if requested == "cpu":
        return torch.device("cpu")
    if requested == "cuda":
        if not torch.cuda.is_available():
            raise RuntimeError("CUDA requested but not available")
        return torch.device("cuda")
    return torch.device("cuda" if torch.cuda.is_available() else "cpu")


def sanitize_output_dir(out_dir: Path, overwrite: bool) -> None:
    if out_dir.exists():
        if not overwrite:
            raise FileExistsError(f"Output directory already exists: {out_dir} (pass --overwrite to replace)")
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)


def get_model_meta(nw_cfg: dict[str, Any], seq_length: int) -> tuple[dict[str, Any], dict[str, Any]]:
    model_meta = copy.deepcopy(config_value(nw_cfg, "configuration.mtlf.task.MODEL_META", {}) or {})
    model_cfg = copy.deepcopy(model_meta.get("model") or {})
    infer_cfg = copy.deepcopy(model_meta.get("inference") or {})

    model_cfg.setdefault("input_size", 10)
    model_cfg.setdefault("output_size", 2)
    model_cfg.setdefault("num_channels", [32, 64, 64, 64])
    model_cfg.setdefault("kernel_size", 2)
    model_cfg.setdefault("dropout", 0.2)

    infer_cfg["seq_length"] = seq_length
    infer_cfg.setdefault("out_seq_len", 1)
    infer_cfg.setdefault("feature_order", [
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
    ])
    infer_cfg.setdefault("output_fields", ["ul_vol", "dl_vol"])
    infer_cfg.setdefault("preprocessing", "log1p_standard_scaler")

    return model_cfg, infer_cfg


def load_group_feature_frame(
    dataset_root: Path,
    group: str,
    period: int,
    cat_duration: int,
    feature_order: list[str],
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    group_dir = dataset_root / group
    if not group_dir.exists():
        raise FileNotFoundError(f"Group directory not found: {group_dir}")

    max_slots = cat_duration // period
    group_data = load_group_slots(
        group_dir,
        period,
        start_offset_sec=0,
        max_slots=max_slots,
        use_pseudo_warmstart=False,
    )
    rows: list[dict[str, Any]] = []
    for slot in group_data.slots:
        row = {
            "groupId": group,
            "slotStart": slot.slot_start,
            "slotEnd": slot.slot_end,
        }
        row.update({name: slot.feature_row[name] for name in feature_order})
        rows.append(row)
    frame = pd.DataFrame(rows)
    return frame, rows


def choose_split_slot(total_slots: int, seq_length: int, train_ratio: float) -> int:
    split_slot = int(total_slots * train_ratio)
    split_slot = max(seq_length + 1, split_slot)
    split_slot = min(total_slots - 1, split_slot)
    return split_slot


def build_group_samples(
    group_frame: pd.DataFrame,
    scaler: StandardScaler,
    feature_order: list[str],
    output_fields: list[str],
    seq_length: int,
    train_ratio: float,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, pd.DataFrame, dict[str, Any]]:
    raw = group_frame[feature_order].to_numpy(dtype=np.float32)
    transformed = scaler.transform(np.log1p(raw))
    output_indices = [feature_order.index(field) for field in output_fields]

    total_slots = len(group_frame)
    if total_slots <= seq_length + 1:
        raise RuntimeError(
            f"group {group_frame['groupId'].iloc[0]} has only {total_slots} slots; "
            f"need more than seq_length+1={seq_length + 1}"
        )

    split_slot = choose_split_slot(total_slots, seq_length, train_ratio)
    train_x: list[np.ndarray] = []
    train_y: list[np.ndarray] = []
    val_x: list[np.ndarray] = []
    val_y: list[np.ndarray] = []
    val_rows: list[dict[str, Any]] = []

    for target_idx in range(seq_length, total_slots):
        x_window = transformed[target_idx - seq_length:target_idx].T.astype(np.float32)
        target_scaled = transformed[target_idx, output_indices].astype(np.float32)
        target_raw = raw[target_idx, output_indices].astype(np.float32)
        target_slot = group_frame.iloc[target_idx]

        if target_idx < split_slot:
            train_x.append(x_window)
            train_y.append(target_scaled)
        else:
            val_x.append(x_window)
            val_y.append(target_scaled)
            val_rows.append({
                "groupId": target_slot["groupId"],
                "targetIndex": int(target_idx),
                "targetSlotStart": target_slot["slotStart"],
                "targetSlotEnd": target_slot["slotEnd"],
                "actualUl": float(target_raw[output_fields.index("ul_vol")]),
                "actualDl": float(target_raw[output_fields.index("dl_vol")]),
            })

    stats = {
        "groupId": group_frame["groupId"].iloc[0],
        "slots": total_slots,
        "splitSlot": split_slot,
        "trainSamples": len(train_x),
        "valSamples": len(val_x),
    }

    if not train_x or not val_x:
        raise RuntimeError(f"group {stats['groupId']} does not have both train and validation samples: {stats}")

    return (
        np.stack(train_x),
        np.stack(train_y),
        np.stack(val_x),
        np.stack(val_y),
        pd.DataFrame(val_rows),
        stats,
    )


def make_criterion(name: str, huber_delta: float) -> nn.Module:
    if name == "mse":
        return nn.MSELoss()
    if name == "mae":
        return nn.L1Loss()
    return nn.HuberLoss(delta=huber_delta)


def mean_loss(model: nn.Module, loader: DataLoader, criterion: nn.Module, device: torch.device) -> float:
    model.eval()
    total = 0.0
    count = 0
    with torch.no_grad():
        for batch_x, batch_y in loader:
            batch_x = batch_x.to(device)
            batch_y = batch_y.to(device)
            loss = criterion(model(batch_x), batch_y)
            batch_size = batch_x.size(0)
            total += float(loss.item()) * batch_size
            count += batch_size
    return total / max(count, 1)


def train_model(
    model: nn.Module,
    train_loader: DataLoader,
    val_loader: DataLoader,
    criterion: nn.Module,
    device: torch.device,
    learning_rate: float,
    weight_decay: float,
    max_epochs: int,
    early_stopping_patience: int,
    lr_patience: int,
    min_delta: float,
) -> tuple[nn.Module, list[dict[str, Any]], int]:
    optimizer = torch.optim.Adam(model.parameters(), lr=learning_rate, weight_decay=weight_decay)
    scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(optimizer, factor=0.5, patience=lr_patience)

    history: list[dict[str, Any]] = []
    best_state = copy.deepcopy(model.state_dict())
    best_val = float("inf")
    best_epoch = 0
    stale_epochs = 0

    for epoch in range(1, max_epochs + 1):
        model.train()
        train_total = 0.0
        train_count = 0
        for batch_x, batch_y in train_loader:
            batch_x = batch_x.to(device)
            batch_y = batch_y.to(device)

            optimizer.zero_grad(set_to_none=True)
            loss = criterion(model(batch_x), batch_y)
            loss.backward()
            optimizer.step()

            batch_size = batch_x.size(0)
            train_total += float(loss.item()) * batch_size
            train_count += batch_size

        train_loss = train_total / max(train_count, 1)
        val_loss = mean_loss(model, val_loader, criterion, device)
        scheduler.step(val_loss)

        current_lr = float(optimizer.param_groups[0]["lr"])
        history.append({
            "epoch": epoch,
            "trainLoss": train_loss,
            "valLoss": val_loss,
            "learningRate": current_lr,
        })

        if val_loss < best_val - min_delta:
            best_val = val_loss
            best_epoch = epoch
            best_state = copy.deepcopy(model.state_dict())
            stale_epochs = 0
        else:
            stale_epochs += 1
            if stale_epochs >= early_stopping_patience:
                break

    model.load_state_dict(best_state)
    return model, history, best_epoch


def invert_outputs(
    scaled_outputs: np.ndarray,
    scaler: StandardScaler,
    feature_order: list[str],
    output_fields: list[str],
) -> np.ndarray:
    output_indices = [feature_order.index(field) for field in output_fields]
    means = scaler.mean_[output_indices]
    scales = scaler.scale_[output_indices]
    unscaled = scaled_outputs * scales + means
    return np.expm1(unscaled)


def collect_val_predictions(
    model: nn.Module,
    val_inputs: np.ndarray,
    val_meta: pd.DataFrame,
    scaler: StandardScaler,
    feature_order: list[str],
    output_fields: list[str],
    device: torch.device,
) -> pd.DataFrame:
    model.eval()
    tensor = torch.tensor(val_inputs, dtype=torch.float32, device=device)
    with torch.no_grad():
        preds_scaled = model(tensor).cpu().numpy()

    preds_raw = invert_outputs(preds_scaled, scaler, feature_order, output_fields)
    result = val_meta.copy()
    result["predUl"] = np.maximum(0.0, preds_raw[:, output_fields.index("ul_vol")])
    result["predDl"] = np.maximum(0.0, preds_raw[:, output_fields.index("dl_vol")])
    result["absErrUl"] = np.abs(result["predUl"] - result["actualUl"])
    result["absErrDl"] = np.abs(result["predDl"] - result["actualDl"])
    return result


def compute_metrics(frame: pd.DataFrame) -> dict[str, float]:
    err_sum = float(frame["absErrUl"].sum() + frame["absErrDl"].sum())
    actual_sum = float(frame["actualUl"].abs().sum() + frame["actualDl"].abs().sum())
    sq_sum = float(((frame["predUl"] - frame["actualUl"]) ** 2).sum() + ((frame["predDl"] - frame["actualDl"]) ** 2).sum())
    point_count = max(len(frame) * 2, 1)
    mae = err_sum / point_count
    mse = sq_sum / point_count
    mean_abs_actual = actual_sum / point_count if point_count else 0.0
    return {
        "mae": mae,
        "mse": mse,
        "rmse": float(np.sqrt(mse)),
        "wape": err_sum / actual_sum if actual_sum > 0 else 0.0,
        "nrmse": float(np.sqrt(mse)) / mean_abs_actual if mean_abs_actual > 0 else 0.0,
    }


def write_bundle(
    bundle_dir: Path,
    model: nn.Module,
    scaler: StandardScaler,
    model_cfg: dict[str, Any],
    infer_cfg: dict[str, Any],
    best_epoch: int,
) -> Path:
    bundle_dir.mkdir(parents=True, exist_ok=True)
    model_script_path = bundle_dir / "model.py"
    model_source = TCN_SOURCE.read_text(encoding="utf-8")
    if "Model = TCNModel" not in model_source:
        model_source = model_source.rstrip() + "\n\nModel = TCNModel\n"
    model_script_path.write_text(model_source, encoding="utf-8")

    weights = [tensor.detach().cpu().numpy() for tensor in model.state_dict().values()]
    np.save(bundle_dir / "model.npy", np.array(weights, dtype=object), allow_pickle=True)
    joblib.dump(scaler, bundle_dir / "scaler.pkl")

    config = {
        "TID": f"initial-local-{uuid.uuid4().hex[:8]}",
        "MODEL_PATH": "model.npy",
        "SCALER_PATH": "scaler.pkl",
        "MODEL_SCRIPT": "model.py",
        "model": model_cfg,
        "inference": infer_cfg,
        "training": {
            "trainer": "local_centralized",
            "best_epoch": best_epoch,
            "local_epochs": best_epoch,
        },
    }
    (bundle_dir / "config.json").write_text(json.dumps(config, indent=2), encoding="utf-8")
    return model_script_path


def main() -> None:
    args = parse_args()
    if not 0.0 < args.train_ratio < 1.0:
        raise ValueError("--train-ratio must be between 0 and 1")

    set_seed(args.seed)
    device = choose_device(args.device)
    sanitize_output_dir(args.out.resolve(), args.overwrite)

    nw_cfg = load_nwdaf_config(args.config)
    model_cfg, infer_cfg = get_model_meta(nw_cfg, args.seq_length)
    feature_order = list(infer_cfg["feature_order"])
    output_fields = list(infer_cfg["output_fields"])

    if "ul_vol" not in output_fields or "dl_vol" not in output_fields:
        raise RuntimeError(f"output_fields must contain ul_vol and dl_vol, got {output_fields}")

    print(f"[train_initial_local] dataset_root={args.dataset_root.resolve()}")
    print(f"[train_initial_local] groups={args.groups} cat_duration={args.cat_duration}s period={args.period}s seq_length={args.seq_length}")
    print(f"[train_initial_local] device={device.type} loss={args.loss} batch_size={args.batch_size} max_epochs={args.max_epochs}")

    group_frames: dict[str, pd.DataFrame] = {}
    group_stats: list[dict[str, Any]] = []
    for group in args.groups:
        frame, _ = load_group_feature_frame(args.dataset_root.resolve(), group, args.period, args.cat_duration, feature_order)
        group_frames[group] = frame
        group_stats.append({"groupId": group, "slots": len(frame)})
        print(f"[train_initial_local] loaded group={group} slots={len(frame)}")

    scaler_rows: list[np.ndarray] = []
    split_stats: list[dict[str, Any]] = []
    for group, frame in group_frames.items():
        split_slot = choose_split_slot(len(frame), args.seq_length, args.train_ratio)
        all_rows = frame[feature_order].to_numpy(dtype=np.float32)
        scaler_rows.append(all_rows)
        split_stats.append({
            "groupId": group,
            "slots": len(frame),
            "splitSlot": split_slot,
            "scalerRows": int(len(all_rows)),
            "trainRows": int(split_slot),
            "valRows": int(len(frame) - split_slot),
        })

    scaler = StandardScaler()
    scaler.fit(np.log1p(np.vstack(scaler_rows)))

    train_x_parts: list[np.ndarray] = []
    train_y_parts: list[np.ndarray] = []
    val_x_parts: list[np.ndarray] = []
    val_y_parts: list[np.ndarray] = []
    val_meta_parts: list[pd.DataFrame] = []
    sample_stats: list[dict[str, Any]] = []

    for group, frame in group_frames.items():
        train_x, train_y, val_x, val_y, val_meta, stats = build_group_samples(
            frame, scaler, feature_order, output_fields, args.seq_length, args.train_ratio
        )
        train_x_parts.append(train_x)
        train_y_parts.append(train_y)
        val_x_parts.append(val_x)
        val_y_parts.append(val_y)
        val_meta_parts.append(val_meta)
        sample_stats.append(stats)
        print(
            f"[train_initial_local] samples group={group} train={stats['trainSamples']} "
            f"val={stats['valSamples']} split_slot={stats['splitSlot']}"
        )

    train_x = np.concatenate(train_x_parts, axis=0)
    train_y = np.concatenate(train_y_parts, axis=0)
    val_x = np.concatenate(val_x_parts, axis=0)
    val_y = np.concatenate(val_y_parts, axis=0)
    val_meta = pd.concat(val_meta_parts, ignore_index=True)

    train_dataset = TensorDataset(
        torch.tensor(train_x, dtype=torch.float32),
        torch.tensor(train_y, dtype=torch.float32),
    )
    val_dataset = TensorDataset(
        torch.tensor(val_x, dtype=torch.float32),
        torch.tensor(val_y, dtype=torch.float32),
    )
    train_loader = DataLoader(train_dataset, batch_size=args.batch_size, shuffle=True)
    val_loader = DataLoader(val_dataset, batch_size=args.batch_size, shuffle=False)

    model = TCNModel(
        input_size=parse_int(model_cfg.get("input_size"), 10) or 10,
        output_size=parse_int(model_cfg.get("output_size"), 2) or 2,
        num_channels=model_cfg.get("num_channels") or [32, 64, 64, 64],
        kernel_size=parse_int(model_cfg.get("kernel_size"), 2) or 2,
        dropout=float(model_cfg.get("dropout", 0.2)),
    ).to(device)
    criterion = make_criterion(args.loss, args.huber_delta)

    wall_start = time.time()
    model, history, best_epoch = train_model(
        model,
        train_loader,
        val_loader,
        criterion,
        device,
        args.learning_rate,
        args.weight_decay,
        args.max_epochs,
        args.early_stopping_patience,
        args.lr_patience,
        args.min_delta,
    )
    wall_duration = time.time() - wall_start

    final_train_loss = mean_loss(model, train_loader, criterion, device)
    final_val_loss = mean_loss(model, val_loader, criterion, device)
    val_predictions = collect_val_predictions(model, val_x, val_meta, scaler, feature_order, output_fields, device)
    overall_metrics = compute_metrics(val_predictions)
    per_group_metrics = {
        group: compute_metrics(frame.reset_index(drop=True))
        for group, frame in val_predictions.groupby("groupId", sort=True)
    }

    bundle_dir = args.out.resolve() / "bundle"
    write_bundle(bundle_dir, model, scaler, model_cfg, infer_cfg, best_epoch)
    val_predictions.to_parquet(args.out.resolve() / "val_predictions.parquet", index=False)

    metrics = {
        "trainLoss": final_train_loss,
        "valLoss": final_val_loss,
        "validation": overall_metrics,
        "validationPerGroup": per_group_metrics,
        "history": history,
    }
    meta = {
        "kind": "initial_local_training",
        "createdAt": pd.Timestamp.now(tz="UTC").isoformat(),
        "datasetRoot": str(args.dataset_root.resolve()),
        "config": str(args.config.resolve()),
        "outDir": str(args.out.resolve()),
        "groups": args.groups,
        "catDurationSec": args.cat_duration,
        "periodSec": args.period,
        "seqLength": args.seq_length,
        "trainRatio": args.train_ratio,
        "loss": args.loss,
        "huberDelta": args.huber_delta,
        "batchSize": args.batch_size,
        "maxEpochs": args.max_epochs,
        "bestEpoch": best_epoch,
        "learningRate": args.learning_rate,
        "weightDecay": args.weight_decay,
        "device": device.type,
        "seed": args.seed,
        "durationSec": wall_duration,
        "groupSlots": group_stats,
        "splitRows": split_stats,
        "sampleStats": sample_stats,
        "trainSamples": int(len(train_dataset)),
        "valSamples": int(len(val_dataset)),
        "bundle": str(bundle_dir),
    }

    (args.out.resolve() / "train_metrics.json").write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    (args.out.resolve() / "meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

    print(f"[train_initial_local] best_epoch={best_epoch} duration_sec={wall_duration:.2f}")
    print(f"[train_initial_local] final_train_loss={final_train_loss:.6f} final_val_loss={final_val_loss:.6f}")
    print(f"[train_initial_local] validation={json.dumps(overall_metrics, sort_keys=True)}")
    print(f"[train_initial_local] bundle={bundle_dir}")


if __name__ == "__main__":
    main()
