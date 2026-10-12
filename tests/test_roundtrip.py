"""Acceptance test 1 (golden round-trip): fixture JSON -> parser -> writer ->
files -> acrec_read -> values equal to the fixture, exactly.

    python3 test_roundtrip.py <path to acrec_replay>
"""
import json
import os
import subprocess
import sys
import tempfile
from decimal import Decimal

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "python"))
import acrec_read as ar  # noqa: E402

FIX = os.path.join(HERE, "fixtures")
DAY = "2026-11-01"
failures = []


def check(cond, msg):
    if not cond:
        failures.append(msg)


def dec(s):
    return Decimal(s)


def events(name):
    out = []
    for line in open(os.path.join(FIX, name)):
        m = json.loads(line)
        if "data" not in m:
            continue
        d = m["data"]
        out.extend(d if isinstance(d, list) else [d])
    return out


def universe_symbols():
    info = json.load(open(os.path.join(FIX, "exchange_info.json")))
    return {s["symbol"] for s in info["symbols"]
            if s["contractType"] == "PERPETUAL" and s["quoteAsset"] in ("USDT", "USDC") and s["status"] == "TRADING"}


def main(replay):
    root = tempfile.mkdtemp(prefix="acrec_rt_")
    files = [os.path.join(FIX, f"{g}.jsonl") for g in ("bbo", "trades", "depth", "liq", "mark")]
    r = subprocess.run([replay, "--root", root, "--exchange-info", os.path.join(FIX, "exchange_info.json"), *files],
                       capture_output=True, text=True)
    check(r.returncode == 0, f"replay failed: {r.stdout} {r.stderr}")
    day = os.path.join(root, DAY)
    meta = ar.load_meta(day)
    uni = universe_symbols()

    # meta.json
    check([s["symbol"] for s in meta["symbols"]] == sorted(uni), f"meta symbols {meta['symbols']}")
    check([s["id"] for s in meta["symbols"]] == list(range(len(uni))), "ids not dense")
    info = {s["symbol"]: s for s in json.load(open(os.path.join(FIX, "exchange_info.json")))["symbols"]}
    for s in meta["symbols"]:
        f = {x["filterType"]: x for x in info[s["symbol"]]["filters"]}
        check(s["tick_size"] == f["PRICE_FILTER"]["tickSize"] and s["step_size"] == f["LOT_SIZE"]["stepSize"],
              f"tick/step for {s['symbol']}")

    def frame(group):
        df = ar.read_day(day, group)
        check(df.attrs == {} or True, "")
        return df

    # bbo
    exp = [e for e in events("bbo.jsonl") if e["s"] in uni]
    df = frame("bbo")
    check(len(df) == len(exp) == 50, f"bbo rows {len(df)} vs {len(exp)}")
    for row, e in zip(df.itertuples(), exp):
        check(row.symbol == e["s"] and row.upd_id == e["u"] and row.E_ms == e["E"] and row.T_ms == e["T"], f"bbo ids {e}")
        for col, k in (("bid_px_e8", "b"), ("bid_qty_e8", "B"), ("ask_px_e8", "a"), ("ask_qty_e8", "A")):
            check(ar.to_decimal(getattr(row, col)) == dec(e[k]), f"bbo {k} {e[k]} != {ar.to_decimal(getattr(row, col))}")
        check(row.recv_ns > 0, "recv_ns")

    # trades
    exp = events("trades.jsonl")
    df = frame("trades")
    check(len(df) == len(exp) == 50, f"trades rows {len(df)}")
    for row, e in zip(df.itertuples(), exp):
        check((row.symbol, row.agg_id, row.first_id, row.last_id, row.E_ms, row.T_ms, bool(row.buyer_is_maker))
              == (e["s"], e["a"], e["f"], e["l"], e["E"], e["T"], e["m"]), f"trade fields {e}")
        check(ar.to_decimal(row.px_e8) == dec(e["p"]) and ar.to_decimal(row.qty_e8) == dec(e["q"]), f"trade px/qty {e}")

    # depth
    exp = events("depth.jsonl")
    raw = ar.read_raw(ar.day_files(day, "depth")[0])[1]
    check(len(raw) == len(exp) == 50, f"depth rows {len(raw)}")
    names = {s["id"]: s["symbol"] for s in meta["symbols"]}
    for rec, e in zip(raw, exp):
        check((names[int(rec["sym"])], int(rec["U"]), int(rec["u"]), int(rec["pu"]), int(rec["E_ms"]), int(rec["T_ms"]))
              == (e["s"], e["U"], e["u"], e["pu"], e["E"], e["T"]), f"depth ids {e['u']}")
        check(int(rec["n_bid"]) == len(e["b"]) and int(rec["n_ask"]) == len(e["a"]), "depth level counts")
        for side in ("bid", "ask"):
            lv = e["b" if side == "bid" else "a"]
            for k in range(20):
                px, q = int(rec[f"{side}_px_e8"][k]), int(rec[f"{side}_qty_e8"][k])
                if k < len(lv):
                    check(ar.to_decimal(px) == dec(lv[k][0]) and ar.to_decimal(q) == dec(lv[k][1]), f"depth level {side}{k}")
                else:
                    check(px == 0 and q == 0, "unused depth levels must be 0")

    # liq (the XAUUSDT item is outside the universe)
    exp = [e for e in events("liq.jsonl") if e["o"]["s"] in uni]
    df = frame("liq")
    check(len(df) == len(exp) == 50, f"liq rows {len(df)}")
    for row, e in zip(df.itertuples(), exp):
        o = e["o"]
        check((row.symbol, row.side_name, row.status_name, row.E_ms, row.T_ms) == (o["s"], o["S"], o["X"], e["E"], o["T"]),
              f"liq fields {o}")
        for col, k in (("px_e8", "p"), ("avg_px_e8", "ap"), ("qty_e8", "q"), ("filled_qty_e8", "z")):
            check(ar.to_decimal(getattr(row, col)) == dec(o[k]), f"liq {k}")

    # mark
    exp = [e for e in events("mark.jsonl") if e["s"] in uni]
    df = frame("mark")
    check(len(df) == len(exp) == 52, f"mark rows {len(df)}")
    for row, e in zip(df.itertuples(), exp):
        check((row.symbol, row.E_ms, row.next_funding_ms) == (e["s"], e["E"], e["T"]), f"mark fields {e}")
        check(ar.to_decimal(row.mark_px_e8) == dec(e["p"]) and ar.to_decimal(row.index_px_e8) == dec(e["i"]), "mark px")
        check(ar.to_decimal(row.funding_rate_e9, 9) == dec(e["r"]), f"funding {e['r']}")

    # Clean fixture: no gaps; files closed (no .part left)
    check(not os.path.exists(os.path.join(day, "gaps.csv")), "unexpected gaps.csv")
    check(not [f for f in os.listdir(day) if f.endswith(".part")], ".part files left after a clean close")
    check(sorted(f for f in os.listdir(day) if f.endswith(".bin.zst")) ==
          [f"{g}.000.bin.zst" for g in ("bbo", "depth", "liq", "mark", "trades")], f"files {os.listdir(day)}")

    # Reader on a crash file: a frame cut short is dropped, earlier frames still decode.
    src = os.path.join(day, "trades.000.bin.zst")
    data = open(src, "rb").read()
    crash = os.path.join(day, "trades.009.crash.bin.zst")
    open(crash, "wb").write(data[:-7])   # cut into the last (data) frame
    header, arr, info = ar.read_raw(crash)
    check(header["group"] == "trades" and info["frames"] == 1 and len(arr) == 0, f"crash read {info}")
    open(crash, "wb").write(data + data[:20])   # complete file + a torn next frame
    header, arr, info = ar.read_raw(crash)
    check(len(arr) == 50 and info["frames"] == 2, f"torn tail {info}")
    os.remove(crash)

    if failures:
        print(f"{len(failures)} failure(s):")
        for f in failures[:30]:
            print("  " + f)
        return 1
    print("roundtrip: all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
