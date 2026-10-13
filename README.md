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
./build/recorder/recorder build/recorder.json
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

## Clone only the recorder

The recorder needs only `recorder/` and the `external/AstraConnect` submodule.
The repo is private, so use a GitHub token (or a deploy key) on the server.

```bash
sudo apt install -y git cmake g++ pkg-config libzstd-dev libboost-system-dev libboost-thread-dev libssl-dev python3-numpy python3-pandas python3-zstandard
git clone --filter=blob:none --sparse https://github.com/Datakan13/Forecasting.git acrec
cd acrec
git sparse-checkout set recorder external
git submodule update --init --depth 1 external/AstraConnect
cmake -S recorder -B build -DRECORDER_BUILD_LIVE=ON
cmake --build build -j
ctest --test-dir build --output-on-failure
```

Results: `build/recorder` and `build/recorder.json` (a copy of `deploy/recorder.json`).
Update later with `git pull && git submodule update --init --depth 1 external/AstraConnect`.

## Deployment (Oracle Cloud ARM, Tokyo/Seoul)

1. Implement ASTRACONNECT_SPEC.md (A2 and B1 are what make it build on aarch64), then build on the server.
2. `sudo useradd -r acrec`, `sudo mkdir -p /opt/acrec /data/acrec`, and `chown acrec /data/acrec`. Copy the `recorder` binary and `deploy/recorder.json` to `/opt/acrec`.
3. Install `deploy/recorder.service` and run `systemctl enable --now recorder`. It uses `Restart=always`, and SIGTERM stops it cleanly.
4. Watch `heartbeat.json`:
   - every enabled group should show `msgs_per_s > 0`
   - `conns_up == conns_total`
   - `warnings` lists disk free under 20 GB and any group quiet for over 5 min
5. The recorder never deletes anything. Uploading and cleanup are handled separately (next section).

**Disk is an estimate, not yet measured.** Uncompressed record bytes at today's
measured rates come to roughly 65 GB/day:
- bbo about 26 GB
- depth about 33 GB for ~60 symbols at 10/s
- trades about 4 GB
- mark about 2 GB

zstd level 3 on this kind of data typically gives 4–8×, so expect about **8–16 GB/day**. That's above the spec's 3–6 GB, and the free tier's 200 GB holds roughly two weeks. Check `bytes_written_today` after the first full day. If it's too high, raise `zstd_level` (costs CPU) or plan a format v2 with delta-encoded fields.

## Upload to Google Drive, then to the PC

```
server: recorder -> /data/acrec/<day>/   --acrec_upload.sh (hourly)-->   Drive: acrec/<day>_00.tar
PC:     acrec_pull.sh (weekly or more)   <--moves, verified--             (Drive freed)
```

**Server side: `deploy/acrec_upload.sh`, run hourly by `acrec-upload.timer`.**
- **What gets uploaded:** every *closed* day, meaning past midnight UTC + 15 min with no `.part` file. Today's folder is never touched.
- **One tar per day:** the day's files are streamed straight into rclone as one `<day>_00.tar`, with `SHA256SUMS_00` inside. No local temp copy is made, and the tar isn't compressed again because the `.bin.zst` files already are.
- **Verification:** the MD5 of the bytes sent must equal the MD5 Drive reports, or nothing is recorded and the next hourly run retries.
- **Late changes:** files that change after upload (a late straggler segment, a rewritten `meta.json`) go up as `<day>_01.tar`, and so on.
- **Local cleanup:** a day is deleted locally only when every file is verified on Drive and the day is older than `KEEP_DAYS` (7).
- **Drive space:** it refuses to upload if Drive would drop below `MIN_DRIVE_FREE_GB` (5). With a 100–200 GB plan that happens after roughly 1–3 weeks unless the PC pulls data off.
- **Status:** `<root>/upload_status.json` (last run, last upload, Drive free GB) and `journalctl -u acrec-upload`.
- It never reads or prints the data itself, only names, sizes and counts.

**PC side: `deploy/acrec_pull.sh DEST` (Linux/WSL).**
- `rclone move` brings the tars down. Drive deletes a tar only after the download's MD5 matches.
- Each tar is unpacked into `DEST/data/` in name order (so `_01` replaces files changed since `_00`) and checked against its `SHA256SUMS`.
- A tar is deleted only once every file in it verifies.
- Run it at least weekly with a 100–200 GB Drive, e.g. from Windows Task Scheduler via `wsl`. Then read the data with `acrec_read.read_day("DEST/data/2026-11-01", "trades")`.

### One-time setup

1. **Install rclone** on the server and the PC. The official build is newer than apt's: `curl https://rclone.org/install.sh | sudo bash`.
2. **Optional: your own Google OAuth client id.** It avoids rclone's shared rate limits. See https://rclone.org/drive/#making-your-own-client-id. Use the same id on the server and the PC.
3. **Create the Drive remote on the server** (it has no browser):
   ```bash
   sudo -u acrec RCLONE_CONFIG=/opt/acrec/rclone.conf rclone config
   # n (new) -> name: gdrive -> storage: drive -> client_id/secret (step 2, or blank)
   # scope: 3  (drive.file: rclone can only see the files it creates)
   # Use auto config? n  -> it prints: rclone authorize "drive" "<…>"
   ```
   Run that `rclone authorize …` command on the PC. A browser opens; log in to your Google account and paste the token it prints back into the server prompt.
4. **Create the same remote on the PC** with plain `rclone config` (the browser works there). Use the same client id; with `drive.file` the PC sees the files the server created.
5. **Install the uploader:**
   ```bash
   sudo cp deploy/acrec_upload.sh deploy/upload.env /opt/acrec/
   sudo chmod +x /opt/acrec/acrec_upload.sh && sudo chown acrec /opt/acrec/rclone.conf
   sudo cp deploy/acrec-upload.service deploy/acrec-upload.timer /etc/systemd/system/
   sudo systemctl daemon-reload && sudo systemctl enable --now acrec-upload.timer
   sudo systemctl start acrec-upload   # one run now; check journalctl -u acrec-upload
   ```
   **Or use one script for both** instead of `recorder.service` + the timer (use one setup or the other, not both):
   `deploy/run_recorder.sh` starts the recorder, restarts it if it exits (5 s backoff, doubling to 5 min on repeated quick failures), and runs `acrec_upload.sh` every hour at :30 UTC. Ctrl-C or SIGTERM stops it cleanly; the recorder gets up to 60 s to close its files.
   ```bash
   sudo cp deploy/run_recorder.sh /opt/acrec/ && sudo chmod +x /opt/acrec/run_recorder.sh
   # as a service:
   sudo cp deploy/run_recorder.service /etc/systemd/system/ && sudo systemctl daemon-reload
   sudo systemctl enable --now run_recorder
   # or by hand (no systemd, e.g. WSL):
   RCLONE_CONFIG=/opt/acrec/rclone.conf nohup /opt/acrec/run_recorder.sh >> /opt/acrec/run_recorder.log 2>&1 &
   ```
   Paths come from the environment: `RECORDER`, `CONFIG`, `UPLOAD`, `UPLOAD_ENV`, `UPLOAD_MINUTE`, plus `UPLOAD_ENABLED=0` to run the recorder only.
6. **Test the whole path once before the real holdout starts:** let a day close, check `<day>_00.tar` appears on Drive, then run `acrec_pull.sh` on the PC and confirm it reports "unpacked and verified".

Oracle's free tier allows 10 TB/month of outbound traffic; 8–16 GB/day is well inside it.

## Status mail every 6 hours

`deploy/acrec_mail.py` mails a plain-text report built from `heartbeat.json` and `upload_status.json`. The subject starts with the status:

| Status | Meaning |
|---|---|
| **DOWN** | `heartbeat.json` missing or older than 5 min: the recorder isn't running |
| **WARN** | heartbeat warnings, fewer connections up than expected, a writer error, an upload error |
| **OK** | none of the above |

The report shows, for each group, messages/s, connections up, seconds quiet and records dropped. It also shows gaps today, GB written today, disk free, writer-queue fill, the last upload and Drive's free space, plus each local day's size and whether it's been uploaded. It contains counts and states only, never prices. It uses only Python's standard library, so there's nothing to install.

**Setup (Gmail as sender):**
1. **App password:** turn on 2-step verification for the sending Google account. Then create an app password at https://myaccount.google.com/apppasswords. Gmail refuses your normal password over SMTP.
2. **Store it:**
   ```bash
   echo 'the16charapppass' | sudo tee /opt/acrec/smtp_password >/dev/null
   sudo chown acrec /opt/acrec/smtp_password && sudo chmod 600 /opt/acrec/smtp_password
   sudo cp deploy/acrec_mail.py deploy/mail.env /opt/acrec/   # then edit MAIL_TO, MAIL_FROM, SMTP_USER
   ```
3. **Test it:**
   - `python3 -I /opt/acrec/acrec_mail.py /opt/acrec/mail.env --dry-run` prints the mail.
   - Without `--dry-run`, it sends one.
4. **Schedule it:**
   - **With `run_recorder.sh`:** nothing more to do. It mails about 2 min after start, so you learn about restarts, then at 00/06/12/18 UTC. Mails stay off while `/opt/acrec/mail.env` doesn't exist. Settings: `MAIL_ENV`, `MAIL_EVERY_H`.
   - **With `recorder.service`:** install `deploy/acrec-mail.service` and `deploy/acrec-mail.timer`, then `systemctl enable --now acrec-mail.timer`.

Oracle Cloud blocks outbound port 25, but port 587 (STARTTLS) and 465 (`SMTP_TLS=ssl`) work. Another provider works too: set `SMTP_HOST`, `SMTP_PORT` and `SMTP_TLS`.
