from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd


DEFAULT_PHASE = "measured-30m"
DEFAULT_SERVER = "server-0"

_AES_TABLE_RE = re.compile(r"^AES-128-CTR\s+(?P<values>.+)$")
_AES_BLOCK_SIZES = [16, 64, 256, 1024, 8192, 16384]


def result_dir(path: str | Path) -> Path:
    resolved = Path(path).expanduser().resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"results directory does not exist: {resolved}")
    if not resolved.is_dir():
        raise NotADirectoryError(f"not a results directory: {resolved}")
    return resolved


def figures_dir(path: str | Path) -> Path:
    out = result_dir(path) / "figures"
    out.mkdir(parents=True, exist_ok=True)
    return out


def load_metadata(path: str | Path) -> dict:
    metadata_path = result_dir(path) / "metadata.json"
    with metadata_path.open() as fh:
        return json.load(fh)


def load_throughput(path: str | Path) -> pd.DataFrame:
    csv_path = result_dir(path) / "throughput.csv"
    df = pd.read_csv(csv_path)
    if df.empty:
        raise ValueError(f"no throughput samples found in {csv_path}")
    df["log_time"] = pd.to_datetime(df["log_time"], format="%Y/%m/%d %H:%M:%S")
    return df.sort_values(["log_time", "phase", "remote", "server"]).reset_index(drop=True)


def load_measured_throughput(
    path: str | Path,
    phase: str = DEFAULT_PHASE,
    server: str = DEFAULT_SERVER,
) -> pd.DataFrame:
    df = load_throughput(path)
    measured = df[(df["phase"] == phase) & (df["server"] == server)].copy()
    if measured.empty:
        raise ValueError(f"no samples found for phase={phase!r}, server={server!r}")
    start = measured["log_time"].min()
    measured["elapsed_seconds"] = (measured["log_time"] - start).dt.total_seconds()
    measured["elapsed_minutes"] = measured["elapsed_seconds"] / 60.0
    return measured.reset_index(drop=True)


def throughput_summary(df: pd.DataFrame) -> dict:
    if df.empty:
        raise ValueError("cannot summarize empty throughput dataframe")

    steady = df.iloc[1:] if len(df) > 1 else df
    duration_minutes = (
        float(df["elapsed_minutes"].max()) if "elapsed_minutes" in df.columns else None
    )

    return {
        "samples": int(len(df)),
        "mean": float(df["rate_req_per_sec"].mean()),
        "stddev": float(df["rate_req_per_sec"].std(ddof=0)),
        "min": float(df["rate_req_per_sec"].min()),
        "max": float(df["rate_req_per_sec"].max()),
        "interval_requests": int(df["interval_requests"].sum()),
        "final_total": int(df["total_since_start"].max()),
        "duration_minutes": duration_minutes,
        "steady_samples": int(len(steady)),
        "steady_mean": float(steady["rate_req_per_sec"].mean()),
        "steady_stddev": float(steady["rate_req_per_sec"].std(ddof=0)),
        "steady_min": float(steady["rate_req_per_sec"].min()),
        "steady_max": float(steady["rate_req_per_sec"].max()),
    }


def _server_name_from_path(path: Path) -> str:
    for part in path.parts:
        if part in {"server-0", "server-1"}:
            return part
    return path.stem.replace("-openssl-speed", "")


def load_aes_speed(path: str | Path) -> pd.DataFrame:
    rows = []
    root = result_dir(path)
    for speed_path in sorted(root.glob("remotes/server-*/riposte-eval/crypto/*openssl-speed.txt")):
        server = _server_name_from_path(speed_path)
        with speed_path.open(errors="replace") as fh:
            for line in fh:
                match = _AES_TABLE_RE.match(line.strip())
                if not match:
                    continue
                values = match.group("values").split()
                if len(values) != len(_AES_BLOCK_SIZES):
                    raise ValueError(f"unexpected AES speed row in {speed_path}: {line.strip()}")
                for block_size, value in zip(_AES_BLOCK_SIZES, values):
                    if not value.endswith("k"):
                        raise ValueError(f"unexpected AES speed value in {speed_path}: {value}")
                    kb_per_sec = float(value[:-1])
                    rows.append(
                        {
                            "server": server,
                            "block_size_bytes": block_size,
                            "kb_per_sec": kb_per_sec,
                            "gb_per_sec": kb_per_sec * 1000.0 / 1_000_000_000.0,
                            "source_file": str(speed_path.relative_to(root)),
                        }
                    )

    if not rows:
        raise ValueError(f"no OpenSSL AES speed output found under {root}")
    return pd.DataFrame(rows)


def aes_reference_summary(df: pd.DataFrame, block_size: int = 16384) -> pd.DataFrame:
    summary = df[df["block_size_bytes"] == block_size].copy()
    if summary.empty:
        raise ValueError(f"no AES rows found for block size {block_size}")
    return summary.sort_values("server").reset_index(drop=True)
