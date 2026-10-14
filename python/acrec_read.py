"""Reader for AstraConnect recorder files (format_version 1).

    import acrec_read as ar
    df = ar.read("2026-11-01/trades.000.bin.zst")            # meta.json found next to the file
    df = ar.read_day("/data/acrec/2026-11-01", "bbo")         # every segment of a day, incl. .crash
    st = ar.read_stats("/data/acrec/2026-11-01")              # per-minute health (stats.csv)

Works on *.crash.bin.zst too: every complete zstd frame is decoded (each is
checksummed); a frame cut short by a crash is dropped.

Prices and quantities are stored as int64 in units of 1e-8, the funding rate in
1e-9. DataFrames keep those exact integers (columns ending in _e8 / _e9) and add
float columns (px, qty, ...) for convenience. Use to_decimal() for exact values.

This module and recorder/include/recorder/records.hpp are the format contract.
The command line prints counts and time ranges only, never prices: the data
is a sealed holdout.
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import struct
import sys
from decimal import Decimal

import numpy as np
import pandas as pd
import zstandard as zstd

MAGIC = b"ACREC\x00\x00\x00"
FORMAT_VERSION = 1
HEADER = struct.Struct("<8sHHII12s")  # magic, format_version, group_id, record_size, yyyymmdd, reserved

GROUPS = {1: "bbo", 2: "trades", 3: "depth", 4: "liq", 5: "mark"}
GROUP_IDS = {v: k for k, v in GROUPS.items()}

# struct formats from the spec (the reference); numpy dtypes below must match them.
STRUCT_FORMATS = {
    "bbo": "<QQQIQqqqq",
    "trades": "<QQQIQQQqqB",
    "depth": "<QQQIQQQBB" + "q" * 80,
    "liq": "<QQQIBqqqqB",
    "mark": "<QQIqqqQ",
}
SIZES = {"bbo": 68, "trades": 69, "depth": 694, "liq": 62, "mark": 52}

_L = 20
DTYPES = {
    "bbo": np.dtype([("E_ms", "<u8"), ("T_ms", "<u8"), ("recv_ns", "<u8"), ("sym", "<u4"), ("upd_id", "<u8"),
                     ("bid_px_e8", "<i8"), ("bid_qty_e8", "<i8"), ("ask_px_e8", "<i8"), ("ask_qty_e8", "<i8")]),
    "trades": np.dtype([("T_ms", "<u8"), ("E_ms", "<u8"), ("recv_ns", "<u8"), ("sym", "<u4"), ("agg_id", "<u8"),
                        ("first_id", "<u8"), ("last_id", "<u8"), ("px_e8", "<i8"), ("qty_e8", "<i8"),
                        ("buyer_is_maker", "u1")]),
    "depth": np.dtype([("E_ms", "<u8"), ("T_ms", "<u8"), ("recv_ns", "<u8"), ("sym", "<u4"), ("U", "<u8"),
                       ("u", "<u8"), ("pu", "<u8"), ("n_bid", "u1"), ("n_ask", "u1"),
                       ("bid_px_e8", "<i8", (_L,)), ("bid_qty_e8", "<i8", (_L,)),
                       ("ask_px_e8", "<i8", (_L,)), ("ask_qty_e8", "<i8", (_L,))]),
    "liq": np.dtype([("E_ms", "<u8"), ("T_ms", "<u8"), ("recv_ns", "<u8"), ("sym", "<u4"), ("side", "u1"),
                     ("px_e8", "<i8"), ("avg_px_e8", "<i8"), ("qty_e8", "<i8"), ("filled_qty_e8", "<i8"),
                     ("status", "u1")]),
    "mark": np.dtype([("E_ms", "<u8"), ("recv_ns", "<u8"), ("sym", "<u4"), ("mark_px_e8", "<i8"),
                      ("index_px_e8", "<i8"), ("funding_rate_e9", "<i8"), ("next_funding_ms", "<u8")]),
}
for _g, _dt in DTYPES.items():
    assert _dt.itemsize == SIZES[_g] == struct.calcsize(STRUCT_FORMATS[_g]), _g
assert HEADER.size == 32


class FormatError(Exception):
    pass


def iter_frames(data: bytes):
    """Yields (payload, complete) per zstd frame. Stops at a truncated or corrupt frame."""
    view = memoryview(data)
    pos = 0
    while pos < len(view):
        d = zstd.ZstdDecompressor().decompressobj()
        try:
            out = d.decompress(view[pos:])
        except zstd.ZstdError:
            return  # corrupt tail (e.g. torn write): stop here
        if not d.eof:
            return  # frame cut short by a crash: its data is not trusted
        consumed = len(view) - pos - len(d.unused_data)
        if consumed <= 0:
            return
        yield out
        pos += consumed


def read_raw(path: str):
    """Returns (header dict, numpy structured array, info dict)."""
    with open(path, "rb") as f:
        data = f.read()
    frames = list(iter_frames(data))
    if not frames:
        raise FormatError(f"{path}: no complete frame")
    blob = b"".join(frames)
    if len(blob) < HEADER.size:
        raise FormatError(f"{path}: shorter than the header")
    magic, version, group_id, rec_size, yyyymmdd, _ = HEADER.unpack_from(blob)
    if magic != MAGIC:
        raise FormatError(f"{path}: bad magic")
    if version != FORMAT_VERSION:
        raise FormatError(f"{path}: format_version {version}, reader supports {FORMAT_VERSION}")
    group = GROUPS.get(group_id)
    if group is None or rec_size != SIZES[group]:
        raise FormatError(f"{path}: group {group_id} / record size {rec_size} not recognised")
    body = memoryview(blob)[HEADER.size:]
    if len(body) % rec_size:
        raise FormatError(f"{path}: {len(body) % rec_size} trailing bytes (partial record)")
    arr = np.frombuffer(body, dtype=DTYPES[group])
    header = {"group": group, "format_version": version, "record_size": rec_size, "yyyymmdd": yyyymmdd}
    info = {"frames": len(frames), "records": len(arr), "crash": ".crash." in os.path.basename(path)}
    return header, arr, info


def load_meta(path_or_dir: str) -> dict:
    p = path_or_dir if path_or_dir.endswith(".json") else os.path.join(path_or_dir, "meta.json")
    with open(p) as f:
        return json.load(f)


def to_decimal(value_e8: int, digits: int = 8) -> Decimal:
    """Exact value of a stored integer (digits=9 for the funding rate)."""
    return Decimal(int(value_e8)).scaleb(-digits)


def _frame(group: str, arr: np.ndarray, meta: dict | None) -> pd.DataFrame:
    cols = {}
    for name in arr.dtype.names:
        a = arr[name]
        if a.ndim == 2:  # depth levels -> bid_px_e8_0 .. bid_px_e8_19
            for i in range(a.shape[1]):
                cols[f"{name}_{i}"] = a[:, i]
        else:
            cols[name] = a
    df = pd.DataFrame(cols)
    for c in list(df.columns):
        if "_e8" in c:
            df[c.replace("_e8", "")] = df[c].to_numpy(dtype=np.float64) / 1e8
        elif c.endswith("_e9"):
            df[c[:-3]] = df[c].to_numpy(dtype=np.float64) / 1e9
    if meta is not None:
        names = {s["id"]: s["symbol"] for s in meta["symbols"]}
        df["symbol"] = df["sym"].map(names)
    if group == "liq":
        codes = (meta or {}).get("liq_status_codes", {})
        df["status_name"] = df["status"].map({int(k): v for k, v in codes.items()})
        df["side_name"] = df["side"].map({0: "BUY", 1: "SELL"})
    return df


def read(path: str, meta: dict | str | None = "auto") -> pd.DataFrame:
    """One file -> DataFrame. meta='auto' loads meta.json from the file's folder."""
    header, arr, info = read_raw(path)
    if isinstance(meta, str):
        mp = os.path.join(os.path.dirname(os.path.abspath(path)), "meta.json") if meta == "auto" else meta
        meta = load_meta(mp) if os.path.exists(mp) else None
    df = _frame(header["group"], arr, meta)
    df.attrs.update(header=header, info=info, path=path)
    return df


def day_files(day_dir: str, group: str) -> list[str]:
    files = glob.glob(os.path.join(day_dir, f"{group}.[0-9][0-9][0-9].bin.zst"))
    files += glob.glob(os.path.join(day_dir, f"{group}.[0-9][0-9][0-9].crash.bin.zst"))
    return sorted(files, key=lambda p: (os.path.basename(p).split(".")[1], p))


def read_day(day_dir: str, group: str) -> pd.DataFrame:
    """All segments of one group for one day (incl. crash files), in segment order."""
    meta = load_meta(day_dir)
    parts = [read(p, meta) for p in day_files(day_dir, group)]
    if not parts:
        return _frame(group, np.zeros(0, dtype=DTYPES[group]), meta)
    return pd.concat(parts, ignore_index=True)


def read_stats(day_dir: str) -> pd.DataFrame:
    """<day>/stats.csv (one row per minute of recorder and machine health) as a
    DataFrame indexed by UTC time. Empty cells (unknown) become NaN."""
    df = pd.read_csv(os.path.join(day_dir, "stats.csv"))
    df["utc_time"] = pd.to_datetime(df["utc_time"], utc=True)
    return df.set_index("utc_time")


def _summary(path: str) -> str:
    header, arr, info = read_raw(path)
    t = arr["E_ms"] if len(arr) else None
    rng = (f"E {pd.to_datetime(int(t.min()), unit='ms')} .. {pd.to_datetime(int(t.max()), unit='ms')}"
           if t is not None else "empty")
    return (f"{path}: {header['group']} v{header['format_version']} day {header['yyyymmdd']} "
            f"records {info['records']} frames {info['frames']} {'(crash) ' if info['crash'] else ''}{rng} "
            f"symbols {len(np.unique(arr['sym'])) if len(arr) else 0}")


def main(argv=None):
    ap = argparse.ArgumentParser(description="Summarize recorder files (counts only, never prices).")
    ap.add_argument("files", nargs="+")
    args = ap.parse_args(argv)
    rc = 0
    for f in args.files:
        try:
            print(_summary(f))
        except (FormatError, OSError) as e:
            print(f"{f}: ERROR {e}", file=sys.stderr)
            rc = 1
    return rc


if __name__ == "__main__":
    sys.exit(main())
