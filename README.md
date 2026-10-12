# Market-data recorder

Records Binance USDⓈ-M futures market data into a **sealed forward dataset**: a
research holdout that nobody browses until it's used. Built on AstraConnect
(websockets, REST, simdjson) and AstraLib (ring buffer, clock). It implements
"AstraConnect market-data recorder: implementation spec (v1)", with the
deviations listed below.

**Status:**
- **Built and tested:** parser, writer, storage, the connection manager (against a fake transport), the Python reader, and all six acceptance tests from the spec.
- **Waiting on you:** the live binary (`recorder`) waits for the AstraConnect/AstraLib additions in [ASTRACONNECT_SPEC.md](ASTRACONNECT_SPEC.md).

## Rules it keeps

1. **Exchange time first.** Every record keeps the exchange's `E`/`T` and the local receive time in ns. Day folders are cut by `E`.
2. **Nothing is lost silently.** Every disconnect, reconnect, stall, sequence gap, parse failure and dropped record becomes a row in `gaps.csv`. Data is never repaired or interpolated. Duplicates are written and flagged.
3. **Lossless.** Decimal strings are parsed exactly into integers, never via `double`.
4. **No prices on screen.** The log, journal, heartbeat and reader CLI show counts, rates and states only. Error messages leave out the offending value.
5. **Crash-safe.** A zstd frame ends and is fsynced at least every 60 s. A crash loses at most the last unfinished frame, and earlier frames stay decodable and checksummed.

## Deviations from spec v1 (and why)

| Spec | Implemented | Why |
|---|---|---|
| Prices in ticks, quantities in lots | **All prices and quantities in int64 units of 1e-8.** Funding rate in 1e-9, as specified. Record sizes and field order unchanged. | Checked live on 2026-10-12: mark, index and estimated settle prices were off-tick in 55–97% of messages, and liquidation `ap` (average price) is off-tick too. Two of 12,574 trade prices were also off-tick. Ticks would have rounded them. Binance sends at most 8 decimals, so 1e-8 is always exact. Tick/step stay in `meta.json`. |
| `bbo` from all-market `!bookTicker` | **Per-symbol `<sym>@bookTicker`** for every perpetual | `!bookTicker` now updates only every 5 s; per-symbol is real-time. Measured about 4,400 msg/s for 563 symbols. |
| "Every USDⓈ-M perpetual" | `contractType PERPETUAL`, quote USDT/USDC, status TRADING (563 today) | Binance also lists 220 `TRADIFI_PERPETUAL` contracts (equities, commodities). Add them via `contract_types` in the config. |
| ~200 streams/connection | Up to 1024 allowed; 150 used | Binance's current limit is 1024. Spec's 150 kept as the default. |
| One websocket URL | `/public/stream` (bookTicker, depth) and `/market/stream` (aggTrade, markPrice, forceOrder) | Binance split the routes. The old unrouted URL delivers only `/public` streams (checked live). |
| `gaps.csv` kinds | Adds `parse_error` | A message that can't be parsed exactly isn't recorded, so it must be logged as a gap. |
| — | `meta.json` adds `units`, `contract_type`, `replaced_by`, `liq_status_codes`, `liq_side_codes` | Makes files self-describing; `replaced_by` links an id to its successor after a tick change. |
| aggTrade `nq` | Not stored | Not in the v1 layout. Add it in a format v2 if needed. |

Other verified facts it relies on:
- depth `pu == previous u`, aggTrade ids +1, and bookTicker `u` strictly increasing all held for 100% of live messages.
- Binance pings every 3 min, needs a pong within 10 min, drops connections at 24 h, and allows 10 incoming messages/s.

## Layout

```
recorder/
  include/recorder/   records.hpp (format contract), decimal, parser, symbols,
                      storage, writer_core, sink, transport, recorder, config, ...
  src/                implementations; queue_sink.cpp + astra_transport.cpp are live-only
  apps/               acrec_replay.cpp (offline), recorder_main.cpp (live daemon)
  python/acrec_read.py   reader: *.bin.zst (incl. .crash) -> pandas
  tests/              recorder_test.cpp, manager_test.cpp, test_roundtrip.py, fixtures/
  deploy/             recorder.json, recorder.service
  ASTRACONNECT_SPEC.md   what AstraConnect/AstraLib need for the live binary
```

**How data flows:**
- One thread per websocket connection: it parses JSON into a record and calls `tryEnqueue` into the AstraLib ring. That never blocks; a full ring means the record is dropped and logged.
- One writer thread: sequence checks, then `Storage`, which writes the zstd segments, `meta.json` and the CSVs.
- One manager thread: shards, subscriptions, reconnects, rotation, REST and the heartbeat.

Writer commands such as "overlap ended" travel in the same ring as the records, so they keep their order.

## Build and test

Needs libzstd ≥ 1.4 (`apt install libzstd-dev`). The tests need Python 3 with
numpy, pandas and zstandard.

```bash
cmake -S . -B build && cmake --build build -j
ctest --test-dir build -R recorder --output-on-failure
```

| Test | Covers |
|---|---|
| `recorder_roundtrip` (spec test 1) | 252 fixture messages → `acrec_replay` → files → `acrec_read.py`; every value equal to the fixture string exactly; reader on a truncated/torn crash file |
| `recorder_test` (spec tests 2–6) | exact `gaps.csv` rows for a broken `pu` link and a skipped agg id; day rollover at 00:00:00.000 by `E`; a forked writer killed mid-segment → `.crash`, next segment number, every complete frame decodes; rotation overlap → one `rotation_overlap` row, no gaps; tick change → new id, `replaced_by`, `events.csv` row; restart keeps the day's ids; decimal and parser edge cases |
| `recorder_manager_test` | fake transport + clock: shard layout and `/public` vs `/market`; depth set (top USDT by 24 h quote volume + all USDC); disconnect → backoff → reconnect rows with exchange times; stall; a full rotation with overlap; listing/delisting → SUBSCRIBE/UNSUBSCRIBE + events; writer backpressure; heartbeat without prices |

The fixtures are made-up values in Binance's live layout (`tests/fixtures/make_fixtures.py`).

**Live binary** (after ASTRACONNECT_SPEC.md A1, B2, B3):

```bash
cmake -S . -B build -DHAR_BUILD_LIVE=ON -DRECORDER_BUILD_LIVE=ON && cmake --build build -j
./build/recorder build/recorder.json
```

## Output

```
<root>/YYYY-MM-DD/   meta.json, {bbo,trades,depth,liq,mark}.NNN.bin.zst, gaps.csv, events.csv, clock.csv
<root>/heartbeat.json   rewritten every 10 s
<root>/recorder.log     rotated text log (also on stderr/journal)
```

- **Segment names:** `.part` while open. A clean close renames to `.bin.zst`; after a crash, startup renames a leftover `.part` to `.crash.bin.zst`. A restart takes the next free segment number and never appends to an existing file.
- **Symbol ids:** dense and stable within a day, also across restarts (reloaded from `meta.json`). A tick/step change gives a new id.
- **Day closing:** a day is closed 2 min after exchange time passes midnight. A later straggler for that day opens a new segment.
- **`gaps.csv` kinds:**
  - `disconnect`, `reconnect`, `stall`: with `last_good_E_ms` and `first_after_E_ms`.
  - `seq_gap`: the detail says which id jumped.
  - `rotation_overlap`: the two E columns hold the overlapping range.
  - `writer_backpressure`: drops or the high-water mark.
  - `parse_error`.
- **`events.csv` kinds:** start, stop, subscribe, unsubscribe, listing, delisting, tick_change, depth_set.
- **`clock.csv`:** `offset_ms = server_time − (send + recv)/2`, every 10 min.

Record layouts are in `include/recorder/records.hpp`, mirrored in `python/acrec_read.py`.
Each file starts with the 32-byte header `ACREC`, version 1, group id, record size and yyyymmdd.

## Reading

```python
import sys; sys.path.insert(0, "recorder/python")
import acrec_read as ar
trades = ar.read_day("/data/acrec/2026-11-01", "trades")   # all segments incl. .crash
# exact integers: px_e8, qty_e8 ...; floats: px, qty ...; ar.to_decimal(x) for exact values
```

`python3 recorder/python/acrec_read.py <files>` prints counts and time ranges only.

## Deployment (Oracle Cloud ARM, Tokyo/Seoul)

1. Implement ASTRACONNECT_SPEC.md (A2 and B1 are what make it build on aarch64), then build on the server.
2. `sudo useradd -r acrec`, `sudo mkdir -p /opt/acrec /data/acrec`, and `chown acrec /data/acrec`. Copy the `recorder` binary and `deploy/recorder.json` to `/opt/acrec`.
3. Install `deploy/recorder.service` and run `systemctl enable --now recorder`. It uses `Restart=always`, and SIGTERM stops it cleanly.
4. Watch `heartbeat.json`:
   - every enabled group should show `msgs_per_s > 0`
   - `conns_up == conns_total`
   - `warnings` lists disk free under 20 GB and any group quiet for over 5 min
5. The recorder never deletes anything. The rclone upload and cleanup are separate.

**Disk is an estimate, not yet measured.** Uncompressed record bytes at today's
measured rates come to roughly 65 GB/day:
- bbo about 26 GB
- depth about 33 GB for ~60 symbols at 10/s
- trades about 4 GB
- mark about 2 GB

zstd level 3 on this kind of data typically gives 4–8×, so expect about **8–16 GB/day**. That's above the spec's 3–6 GB, and the free tier's 200 GB holds roughly two weeks. Check `bytes_written_today` after the first full day. If it's too high, raise `zstd_level` (costs CPU) or plan a format v2 with delta-encoded fields.
