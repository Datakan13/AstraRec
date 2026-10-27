# AstraRec

AstraRec records live Binance USDⓈ-M futures market data (best bid/offer, aggregated
trades, top-20 order-book depth, liquidations and mark price) for every USDT/USDC
perpetual contract. It writes the data into a **sealed forward dataset**: a research
holdout that nobody browses until it's used. It handles about 7–15k messages per
second on a single shared vCPU, stores zstd-compressed hourly segments with exact
fixed-point prices, and uploads and verifies each finished hour off-site.

It's built on [AstraConnect](https://github.com/Datakan13/AstraConnect) (websockets,
REST, simdjson) and AstraLib (ring buffer, clock; vendored in AstraConnect). It
implements "AstraConnect market-data recorder: implementation spec (v1)", with the
deviations listed below.

**Status:** running 24/7 since 2026-10-14 on a 1 GB Hetzner Cloud VM (Ubuntu 26.04),
with hourly uploads to a Hetzner Storage Box. The parser, writer, storage, the
connection manager (against a fake transport), the Python reader and all six
acceptance tests from the spec are covered by the test suite.

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
include/recorder/   records.hpp (format contract), decimal, parser, symbols,
                    storage, writer_core, sink, transport, recorder, config, ...
src/                implementations; queue_sink.cpp + astra_transport.cpp are live-only
apps/               acrec_replay.cpp (offline), recorder_main.cpp (live daemon)
python/acrec_read.py   reader: *.bin.zst (incl. .crash) -> pandas
tests/              recorder_test.cpp, manager_test.cpp, test_roundtrip.py, fixtures/
deploy/             recorder.json, systemd units/timers, upload, pull and mail scripts
external/AstraConnect  submodule (includes AstraLib and simdjson)
ASTRACONNECT_SPEC.md   what the recorder needed from AstraConnect/AstraLib (implemented in 662dbf0)
```

**How data flows:**
- One thread per websocket connection: it parses JSON into a record and calls `tryEnqueue` into the AstraLib ring. That never blocks; a full ring means the record is dropped and logged.
- One writer thread: sequence checks, then `Storage`, which writes the zstd segments, `meta.json` and the CSVs.
- One manager thread: shards, subscriptions, reconnects, rotation, REST and the heartbeat.

Writer commands such as "overlap ended" travel in the same ring as the records, so they keep their order.

## Build and test

Needs CMake ≥ 3.22, a C++20 compiler and libzstd ≥ 1.4 (`apt install libzstd-dev`).
The live binary also needs Boost ≥ 1.74 (headers only) and OpenSSL 3. The tests
need Python 3 with numpy, pandas and zstandard.

```bash
sudo apt install -y git cmake g++ pkg-config libzstd-dev libboost-dev libssl-dev python3-numpy python3-pandas python3-zstandard
git clone --recurse-submodules https://github.com/Datakan13/AstraRec.git
cd AstraRec
cmake -S . -B build && cmake --build build -j
ctest --test-dir build --output-on-failure
```

| Test | Covers |
|---|---|
| `recorder_roundtrip` (spec test 1) | 252 fixture messages → `acrec_replay` → files → `acrec_read.py`; every value equal to the fixture string exactly; reader on a truncated/torn crash file |
| `recorder_test` (spec tests 2–6) | exact `gaps.csv` rows for a broken `pu` link and a skipped agg id; day rollover at 00:00:00.000 by `E`; a forked writer killed mid-segment → `.crash`, next segment number, every complete frame decodes; rotation overlap → one `rotation_overlap` row, no gaps; tick change → new id, `replaced_by`, `events.csv` row; restart keeps the day's ids; hourly segments close exactly at the hour and a restart skips `.pruned` numbers; decimal and parser edge cases |
| `recorder_manager_test` | fake transport + clock: shard layout and `/public` vs `/market`; depth set (top USDT by 24 h quote volume + all USDC); disconnect → backoff → reconnect rows with exchange times; stall; a full rotation with overlap; listing/delisting → SUBSCRIBE/UNSUBSCRIBE + events; writer backpressure; heartbeat without prices |

The fixtures are made-up values in Binance's live layout (`tests/fixtures/make_fixtures.py`).

**Live binary:**

```bash
cmake -S . -B build -DRECORDER_BUILD_LIVE=ON && cmake --build build -j
./build/recorder build/recorder.json      # build/recorder.json is a copy of deploy/recorder.json
```

Binance refuses US IP addresses (HTTP 451), so the live binary must run from a region Binance serves.

## Output

```
<root>/YYYY-MM-DD/   meta.json, {bbo,trades,depth,liq,mark}.NNN.bin.zst, gaps.csv, events.csv, clock.csv
<root>/heartbeat.json   rewritten every 10 s
<root>/recorder.log     rotated text log (also on stderr/journal)
```

- **Segment names:** `.part` while open. A clean close renames to `.bin.zst`; after a crash, startup renames a leftover `.part` to `.crash.bin.zst`. A restart takes the next free segment number and never appends to an existing file.
- **Hourly segments** (`"segment_minutes": 60`, the deploy default): the first record of each new hour of exchange time closes every open segment, so `bbo.000` holds 00:00–01:00, `bbo.001` 01:00–02:00, and so on (plus extra numbers after restarts). Finished hours can then be uploaded and deleted locally during the day. With `0`, one segment covers the whole day, as in spec v1.
- **`<name>.pruned`:** an empty marker the uploader leaves after deleting a verified segment locally. It keeps that segment number taken, so a restart never reuses it.
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

## Diagnosing problems: `stats.csv`

Every minute the recorder appends one row to `<day>/stats.csv`. It's uploaded with the day's other files, so the full history ends up on the PC. Counts and states only, never prices. Empty cell = unknown.

| Columns | What they show |
|---|---|
| `<group>_msgs_per_s`, `_conns_up`/`_conns_total`, `_dropped`, `_quiet_s` | per group: rate over the minute, live connections, records dropped in the minute, seconds since the last message |
| `disconnects`, `reconnects`, `stalls`, `parse_errors` | events in the minute (details and exchange times in `gaps.csv`) |
| `queue_fill`, `queue_fill_max`, `queue_capacity` | writer queue now and its peak in the minute |
| `flush_ms_max` | slowest frame end + fsync in the minute: how long the disk took |
| `bytes_written_today`, `gaps_today`, `disk_free_gb` | running totals |
| `proc_cpu_pct`, `proc_rss_mb`, `proc_threads` | the recorder process: CPU (100 = one full core), memory |
| `sys_cpu_busy_pct`, `sys_iowait_pct`, `sys_steal_pct`, `load1` | the machine: busy CPU, CPU waiting on disk, CPU taken by the cloud host |
| `mem_avail_mb`, `swap_used_mb` | free memory, swap in use |
| `net_rx_mbps`, `tcp_retrans_pct` | received traffic, share of sent TCP segments retransmitted |

**Reading it: hardware or software?**

| Symptom (in the same minutes) | Likely cause |
|---|---|
| `queue_fill_max` climbing, `proc_cpu_pct` near 100, `sys_steal_pct` low | the recorder needs more CPU than the machine has (load spike or code), so a bigger server is needed |
| `queue_fill_max` climbing, `sys_steal_pct` high (> ~20) | **host**: the cloud provider is giving the shared vCPU to other tenants |
| `flush_ms_max` in seconds, `sys_iowait_pct` high | **disk** is slow |
| `mem_avail_mb` falling, `swap_used_mb` rising, `proc_rss_mb` steady | something else on the server is using memory |
| `proc_rss_mb` rising day after day | a **memory leak in the recorder** (software) |
| `disconnects` with `tcp_retrans_pct` up and `net_rx_mbps` dropping | **network** between the server and Binance |
| `disconnects` with clean TCP, every group at once | Binance closed connections (maintenance, their limits) |
| `disconnects` with clean TCP, one connection repeatedly | that connection's streams or the software: check `gaps.csv` reasons and `recorder.log` |
| `stalls` with `msgs_per_s` 0 for one group only | that group's streams went quiet (Binance side) |
| `*_dropped` > 0 | records lost: always together with a full queue, so look at the queue rows above |

```python
st = ar.read_stats("DEST/data/2026-11-01")
st[["bbo_msgs_per_s", "queue_fill_max", "proc_cpu_pct", "sys_steal_pct"]].plot(subplots=True)
st[st.disconnects > 0]          # the minutes with disconnects
```

The status mail summarizes the last 6 hours of these rows: averages and minimums per group, drops, disconnects, the peak queue, the slowest flush, CPU, steal, memory and retransmits. It turns WARN on drops, a queue more than half full, stalls or more than 6 disconnects, or a flush over 5 s.

## Reading

```python
import sys; sys.path.insert(0, "python")
import acrec_read as ar
trades = ar.read_day("/data/acrec/2026-11-01", "trades")   # all segments incl. .crash
# exact integers: px_e8, qty_e8 ...; floats: px, qty ...; ar.to_decimal(x) for exact values
```

`python3 python/acrec_read.py <files>` prints counts and time ranges only.

## Deployment

Runs on any Linux server in a region Binance serves (US IPs get HTTP 451). The
current server is a **Hetzner Cloud 1 GB, 1 vCPU, 19 GB disk VM in Helsinki**; see
"Small server" below for what that needs.

1. Build on the server with `-DRECORDER_BUILD_LIVE=ON` ("Build and test"). One file needs about 1.5 GB of compiler memory, so add swap on small servers. Update later with `git pull && git submodule update --init external/AstraConnect`, rebuild, then `install -m 755` the binary (a plain `cp` over a running binary fails with "Text file busy").
2. `sudo useradd -r -s /usr/sbin/nologin acrec`, `sudo mkdir -p /opt/acrec /data/acrec`, and `chown acrec /data/acrec`. Copy the `recorder` binary and `deploy/recorder.json` to `/opt/acrec`. Leave `/opt/acrec` and the programs in it owned by root: the recorder runs as `acrec` and only needs to read them.
3. Install `deploy/run_recorder.service` (recorder + hourly upload + status mail) and run `systemctl enable --now run_recorder`. It uses `Restart=always`, and SIGTERM stops it cleanly. `deploy/recorder.service` is the recorder alone, for use with `acrec-upload.timer` instead.

**Security.** `run_recorder.service` runs as the unprivileged user `acrec` inside a systemd sandbox. The service can:
- write only `/data/acrec` and its own home, `/var/lib/acrec` (created by systemd, used by rclone and Python)
- read its programs, configs and keys in `/opt/acrec`, but not change them

It can't:
- see `/root`, `/home` or other users' processes
- gain privileges or use raw sockets

`systemd-analyze security run_recorder` rates it about **2/10 exposure ("OK")**. It deliberately leaves out `SystemCallArchitectures=native` (and `SystemCallFilter`): on Ubuntu 26.04 (systemd 258, glibc 2.42) that setting makes `tar`'s stat call fail with "Function not implemented", which broke every upload in October 2026 until it was found by removing sandbox settings one at a time. An unhardened service rates about 9.6. Secrets (`storagebox_ed25519`, `rclone.conf`, `smtp_password`) are owned by `acrec` with mode `600`. A systemd *user* instance (`systemctl --user`) would not be safer: user units can use fewer of these protections.
4. Watch `heartbeat.json`:
   - every enabled group should show `msgs_per_s > 0`
   - `conns_up == conns_total`
   - `warnings` lists disk free under `min_disk_free_gb` and any group quiet for over 5 min
5. The recorder never deletes anything. Uploading and cleanup are handled separately (next section).

**Disk is an estimate, not yet measured.** Uncompressed record bytes at today's
measured rates come to roughly 65 GB/day:
- bbo about 26 GB
- depth about 33 GB for ~60 symbols at 10/s
- trades about 4 GB
- mark about 2 GB

zstd level 3 on this kind of data typically gives 4–8×, so expect about **8–16 GB/day**. That's above the spec's 3–6 GB. Check `bytes_written_today` after the first full day. If it's too high, raise `zstd_level` (costs CPU) or plan a format v2 with delta-encoded fields.

### Small server (Hetzner 1 GB / 1 vCPU / 19 GB disk)

The deploy defaults are set for this:

| Setting | Value | Why |
|---|---|---|
| `recorder.json` `segment_minutes` | 60 | each finished hour can be uploaded during the day |
| `recorder.json` `min_disk_free_gb` | 3 | the heartbeat warning threshold (20 would always warn on a 19 GB disk) |
| `upload.env` `KEEP_LOCAL_HOURS` | 0 | a segment is deleted locally on the run after its verified upload |
| `upload.env` `KEEP_DAYS` | 0 | a closed day's folder (meta, CSVs, markers) goes on the run after its last upload |

Segments close at :00 and the upload runs at :30. A segment goes up at the first :30 after it closes and is deleted at the next one. So the disk holds about 1.5–2.5 hours of data, roughly 0.5–2 GB, instead of 1–2 days.

**Building:** `astra_transport.cpp` needs about 1.0 GB of compiler memory on GCC 15. That's with the low-memory GCC flags `CMakeLists.txt` sets for it; without them it's 1.5 GB. A 1 GB server has about 760 MB free, so add a 2 GB swap file **and check it's active** before building, then build one file at a time:
```bash
[ -f /swapfile ] || { fallocate -l 2G /swapfile && chmod 600 /swapfile && mkswap /swapfile; }
swapon /swapfile 2>/dev/null; grep -q '^/swapfile ' /etc/fstab || echo '/swapfile none swap sw 0 0' >> /etc/fstab
swapon --show                      # must list /swapfile, or the compiler gets killed
cmake --build build -j1            # resumes where it stopped; that one file takes a few minutes
```

**Watch on the first live run:**
- In `heartbeat.json`, `writer_queue.fill` should stay near 0 and `dropped_total` at 0. If either climbs, the single shared vCPU isn't keeping up.
- `bytes_written_today` gives the real daily size.

If the Storage Box or the network is down for long, segments pile up locally (about 0.3–0.7 GB per hour). The mail turns WARN when disk free drops below `min_disk_free_gb`.

## Upload to the Hetzner Storage Box, then to the PC

```
server: recorder -> /data/acrec/<day>/  --acrec_upload.sh (hourly, SFTP)-->  Storage Box: acrec/<day>_000.tar, _001.tar, ...
                                                                            acrec/gaps/<day>_gaps.csv
PC:     acrec_pull.sh (as often as you like)  <--copies (or moves), verified--  Storage Box (the archive)
```

Transfers use rclone's SFTP backend on the box's port 23, logging in with an SSH key, with the box's host key pinned. Any other rclone remote works too: set `REMOTE`.

**Server side: `deploy/acrec_upload.sh`, run hourly (by `run_recorder.sh` or `acrec-upload.timer`).**
- **What gets uploaded:**
  - **Finished segments at any time,** including today's. With hourly segments, each hour goes up in the run after it ends.
  - **`meta.json` and the CSVs** (`gaps`, `events`, `clock`, `stats`) only once the day is closed (past midnight UTC + 15 min), because they change until then.
  - **Never** a `.part` file that's still being written.
  - **`gaps.csv` also as a plain file, every run:** it's copied to `acrec/gaps/<day>_gaps.csv` whenever it has changed (MD5-checked; state in `<day>/.gaps_uploaded`), so today's gaps can be seen on the box without waiting for midnight or unpacking a tar. That file is replaced as the day goes on; the final one also goes into the day's tar. If this copy fails, the data upload goes ahead anyway, `upload_status.json` says `warn` and a mail goes out.
- **One tar per run per day:** new files are streamed straight into rclone as `<day>_000.tar`, then `<day>_001.tar`, and so on, each with its `SHA256SUMS_NNN` inside. No local temp copy is made, and the tar isn't compressed again because the `.bin.zst` files already are. An existing name on the box is never overwritten; the next number is used instead.
- **Verification:** the MD5 of the bytes sent must equal the MD5 the box computes itself (`md5sum` over SSH), or nothing is recorded and the next hourly run retries.
- **Late changes:** files that change after upload (a late straggler segment, a rewritten `meta.json`) go up again in the next part.
- **Local cleanup** (only ever for files verified on the box):
  - **Segments:** a segment verified in an earlier run and older than `KEEP_LOCAL_HOURS` is deleted and leaves a `.pruned` marker. `-1` keeps segments until the whole day goes.
  - **Days:** a closed day is deleted once every file is verified and the day is older than `KEEP_DAYS`. The deploy default for both is `0`; see "Small server".
- **Box space:** it refuses to upload if the box would drop below `MIN_REMOTE_FREE_GB` (5), read with `df` over SSH. At about 11 GB/day, 1 TB lasts roughly 3 months.
- **Connections:** the box allows 10 simultaneous connections per account. The scripts use at most 2–3 at once.
- **Status:** `<root>/upload_status.json` (last run, last upload, box free GB) and the logs (`journalctl -u run_recorder`, or `-u acrec-upload`).
- It never reads or prints the data itself, only names, sizes and counts.

**PC side: `deploy/acrec_pull.sh DEST` (Linux/WSL).**
- **Default (`PULL_MODE=copy`):** downloads every tar it doesn't have yet. The box keeps everything as the archive. `DEST/.pulled` remembers what's done, so each tar is fetched once.
- **`PULL_MODE=move`:** also deletes from the box the tars the PC has unpacked and verified. Use it when the box gets full. Nothing is deleted from the box before every file in its tar matched `SHA256SUMS` on the PC.
- Each tar is unpacked into `DEST/data/` in name order (so a later part replaces files changed since an earlier one) and checked against its `SHA256SUMS_NNN`. A tar is deleted from `DEST/incoming/` only once every file in it verifies.
- It also copies the box's `gaps/` folder to `DEST/gaps/` (refreshed each run, never deleted from the box).
- Then read the data with `acrec_read.read_day("DEST/data/2026-11-01", "trades")`.

### One-time setup

1. **Turn on SSH for the box.** In the Hetzner Console, open the Storage Box and enable **SSH support**; also enable **External reachability** if the option is there. Note the username `uXXXXXX`; the host is `uXXXXXX.your-storagebox.de`. It can take a few minutes to become reachable.
2. **Install rclone** on the server and the PC. The official build is newer than apt's: `curl https://rclone.org/install.sh | sudo bash`.
3. **Give the server a key for the box** (as root; the `acrec` user reads it):
   ```bash
   BOX=uXXXXXX                                      # your Storage Box username
   ssh-keygen -t ed25519 -N "" -f /opt/acrec/storagebox_ed25519 -C acrec-recorder
   cat /opt/acrec/storagebox_ed25519.pub | ssh -p23 $BOX@$BOX.your-storagebox.de install-ssh-key   # asks for the box password once
   ssh-keyscan -p 23 $BOX.your-storagebox.de > /opt/acrec/storagebox_known_hosts                  # pins the box's host key
   chown acrec /opt/acrec/storagebox_ed25519 /opt/acrec/storagebox_known_hosts && chmod 600 /opt/acrec/storagebox_ed25519
   ```
4. **Create the rclone remote** `storagebox`:
   ```bash
   export RCLONE_CONFIG=/opt/acrec/rclone.conf
   rclone config create storagebox sftp host=$BOX.your-storagebox.de user=$BOX port=23 \
     key_file=/opt/acrec/storagebox_ed25519 known_hosts_file=/opt/acrec/storagebox_known_hosts \
     shell_type=unix md5sum_command=md5sum sha1sum_command=sha1sum
   chown acrec /opt/acrec/rclone.conf
   rclone mkdir storagebox:acrec          # no leading "/" in Storage Box paths
   rclone about storagebox:               # shows the box's total and free space
   echo test > /tmp/t && rclone copyto /tmp/t storagebox:acrec/t && rclone md5sum storagebox:acrec/t && md5sum /tmp/t && rclone deletefile storagebox:acrec/t
   ```
   The two MD5 lines must match: that's the check the uploader relies on.
5. **On the PC**, do the same with its own key: run steps 3–4 with paths in your home folder, e.g. `~/.ssh/storagebox_ed25519`. `install-ssh-key` adds keys without removing the server's.
6. **Install the uploader:**
   ```bash
   sudo cp deploy/acrec_upload.sh deploy/upload.env /opt/acrec/
   sudo chmod +x /opt/acrec/acrec_upload.sh
   sudo -u acrec env RCLONE_CONFIG=/opt/acrec/rclone.conf /opt/acrec/acrec_upload.sh /opt/acrec/upload.env   # one run by hand
   ```
   Then **either** use one script for recorder + uploads (recommended):
   `deploy/run_recorder.sh` starts the recorder, restarts it if it exits (5 s backoff, doubling to 5 min on repeated quick failures), and runs `acrec_upload.sh` every hour at :30 UTC. Ctrl-C or SIGTERM stops it cleanly; the recorder gets up to 60 s to close its files.
   ```bash
   sudo cp deploy/run_recorder.sh /opt/acrec/ && sudo chmod +x /opt/acrec/run_recorder.sh
   sudo cp deploy/run_recorder.service /etc/systemd/system/ && sudo systemctl daemon-reload
   sudo systemctl enable --now run_recorder
   # or by hand (no systemd):
   RCLONE_CONFIG=/opt/acrec/rclone.conf nohup /opt/acrec/run_recorder.sh >> /opt/acrec/run_recorder.log 2>&1 &
   ```
   Paths come from the environment: `RECORDER`, `CONFIG`, `UPLOAD`, `UPLOAD_ENV`, `UPLOAD_MINUTE`, plus `UPLOAD_ENABLED=0` to run the recorder only.
   **Or** `recorder.service` plus `acrec-upload.service` and `acrec-upload.timer` (`systemctl enable --now acrec-upload.timer`). Use one setup or the other, not both.
7. **Test the whole path once before the real holdout starts:** after the first full hour, check `rclone lsf storagebox:acrec` lists `<day>_000.tar` and that the segment is replaced by a `.pruned` marker on the next run. Then run `acrec_pull.sh` on the PC and confirm it reports "unpacked and verified".

Traffic: 8–16 GB/day is about 0.5 TB/month, well inside Hetzner's 20 TB; traffic to a Storage Box is free.

## Status mail every 6 hours

`deploy/acrec_mail.py` mails a plain-text report built from `heartbeat.json` and `upload_status.json`. The subject starts with the status:

| Status | Meaning |
|---|---|
| **DOWN** | `heartbeat.json` missing or older than 5 min: the recorder isn't running |
| **WARN** | heartbeat warnings, fewer connections up than expected, a writer error, an upload error |
| **OK** | none of the above |

The report shows, for each group, messages/s, connections up, seconds quiet and records dropped. It also shows gaps today, GB written today, disk free, writer-queue fill, the last upload and the Storage Box's free space, plus each local day's size and whether it's been uploaded. It contains counts and states only, never prices. It uses only Python's standard library, so there's nothing to install.

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

Hetzner Cloud blocks outgoing ports 25 and 465 on new servers, so keep port 587 with `SMTP_TLS=starttls` (the default). Another provider works too: set `SMTP_HOST`, `SMTP_PORT` and `SMTP_TLS`.

## Notes

The design is the creator's: the storage format, file layout, segmenting and
upload scheme were the creator's choices. Most of the code, tests and documentation
implementing that design were written by AI, directed and reviewed by the creator.
The deployment, which means setting up and running the recorder on the production
server and the Storage Box, was largely done by the creator.

## License

Copyright (C) 2026 Atakan Burak Bacanlı.

AstraRec is free software under the [GNU Affero General Public License v3.0](LICENSE).
You may use, study, change and share it. If you distribute it, or run a modified
version that others use over a network, you must publish your source under the
same license. The libraries it builds on, AstraConnect and AstraLib, stay under
Apache-2.0.
