# AstraConnect / AstraLib additions for the recorder

What the market-data recorder (`recorder/`) needs from AstraConnect and AstraLib,
and what else turned up along the way. Everything here was checked against
AstraConnect `b7269ec` and AstraLib `9b41c62` on 2026-10-12. The Binance facts
were checked live the same day.

The recorder's offline parts already build and pass their tests. The live binary
(`-DRECORDER_BUILD_LIVE=ON`) is written against the interfaces below and
compiles once **A1, B2 and B3** exist. **A2 and B1** are needed to run it on the
Oracle ARM (aarch64) server.

| # | Change | Blocks |
|---|---|---|
| A1 | `AtomicRingBuffer`: `tryEnqueue`, `tryDequeue`, `approxSize` | live build |
| A2 | Portable `cpuRelax()` instead of `_mm_pause`; x86-only `Timer` | ARM |
| B1 | Use A2 everywhere in AstraConnect | ARM |
| B2 | `AstraConnect::RawWebsocket`: generic stream connection | live build |
| B3 | `Futures::API::fetchRaw`: raw REST GET with HTTP status | live build |
| B4 | Move existing streams to the routed `/public` `/market` URLs | existing bug, not the recorder |
| B5 | Small fixes: `inline` in a header, HTTP status ignored | hygiene |

Where the recorder uses these:
- A1: `recorder/src/queue_sink.cpp`
- B2, B3: `recorder/src/astra_transport.cpp`

The recorder's own interface is in `recorder/include/recorder/transport.hpp`.

---

## A1. `AtomicRingBuffer`: non-blocking enqueue and dequeue

**Why.** `enqueue` takes a ticket with `fetch_add` and then spins until its slot
is free, so a full buffer blocks the producer. The recorder's network threads
must never block: when the writer falls behind, the spec says to drop the
record and log `writer_backpressure`.

**Interface** (in `AstraLib::Buffers::AtomicRingBuffer<A, SIZE, BATCH_SIZE>`):

```cpp
// False if the buffer is full; never spins waiting for space.
bool tryEnqueue(const A& data);
bool tryEnqueue(A&& data);
// False if the buffer is empty; never spins waiting for data.
bool tryDequeue(A& out);
// Items currently queued, approximately (writeTicket - readTicket, clamped at 0).
size_t approxSize() const;
```

**Semantics.**
- Safe with many producers and one consumer (MPSC). MPMC is fine too.
- A failed call changes nothing: it doesn't consume a ticket.
- Mixing `enqueue` and `tryEnqueue` on the same buffer must stay correct.
- Implementation hint (the standard Vyukov scheme on your existing `seq` fields):
  - **tryEnqueue:** load `writeTicket`. If `slot.seq == ticket`, take the ticket with a CAS on `writeTicket`. If `seq < ticket`, the buffer is full: return false. Otherwise reload and retry.
  - **tryDequeue:** the same idea with `readTicket` and `seq == ticket + 1`.
- Keep the slot size limit of the vendored copy (`sizeof(T) <= 4096`). The
  recorder's queue item is about 710 bytes, and it's used as
  `AtomicRingBuffer<Item, 65536>` allocated on the heap.

**Also found in standalone AstraLib (HEAD `9b41c62`):**
- `batchDequeue` is unfinished and doesn't compile, for example `target = writeTicket.value.` with nothing after it.
- `Slot` asserts `sizeof(T) <= 128 - sizeof(seq)`, which would reject the recorder's item.
- The copy vendored in AstraConnect has neither problem.

**Tests to add:**
- `tryEnqueue` on a full buffer returns false and leaves it intact.
- `tryDequeue` on an empty buffer returns false.
- 4 producers × 1M items with a slow consumer: every item that `tryEnqueue` accepted comes out exactly once and in per-producer order, and the rejected count adds up.
- Run it under TSAN.

## A2. Portable spin pause (AstraLib)

**Why.** `_mm_pause` and `<immintrin.h>` only exist on x86, but the recorder
runs on an Oracle Ampere (aarch64) server. Futexes (`SYS_futex`) are Linux
syscalls and work on aarch64 unchanged.

**Interface:**

```cpp
// <AstraLib/Atomic/cpuRelax.hpp>
namespace AstraLib::Atomic {
inline void cpuRelax() noexcept {
#if defined(__x86_64__) || defined(__i386__)
  _mm_pause();                                   // needs <immintrin.h> inside this #if
#elif defined(__aarch64__)
  asm volatile("yield" ::: "memory");
#else
  std::atomic_signal_fence(std::memory_order_seq_cst);
#endif
}
}
```

**Where:**
- Replace `_mm_pause()` and `#include <immintrin.h>` in `Atomic/spinlock.hpp` and `Buffers/atomicRingBuffer.hpp`.
- `Time/timer.hpp` uses `rdtscp`/`cpuid`, which are x86-only. Either guard it with the same `#if` and leave it out of `AstraLib.hpp` on other architectures, or implement it on aarch64 with `CNTVCT_EL0` / `CNTFRQ_EL0`.
- The recorder includes only `Buffers/atomicRingBuffer.hpp` and `Time/timeNow.hpp`, never `AstraLib.hpp`, so `Timer` doesn't block it. AstraConnect's `apiManager.hpp` does include `AstraLib.hpp`, though.
- Update the README's "x86 only" requirement and add an aarch64 CI job (`ubuntu-24.04-arm`).

## B1. AstraConnect on ARM

This applies A2 inside AstraConnect: replace `_mm_pause()` with `AstraLib::Atomic::cpuRelax()` in

- `src/core/streams/websocketStreamHolder.hpp:125`
- `src/api/futures/streams/tradeEventStream.hpp:28`
- `src/api/futures/streams/markPriceStream.hpp:29`
- `src/api/user/streams/userFuturesStream.hpp:80`
- `src/api/orderbook/orderbook.hpp:120`

Also bump the vendored AstraLib once A1 and A2 are in.

## B2. `AstraConnect::RawWebsocket`: a generic stream connection

**Why.** `WebsocketStreamHolder` is built for one fixed stream with a parse
function. The recorder runs about 11 combined-stream connections with about 1,200
streams, and it needs:
- SUBSCRIBE/UNSUBSCRIBE on a live connection, for listings and delistings.
- To be told about every disconnect, so it can be logged. The holder reconnects silently.
- To decide itself when to reconnect: backoff, and rotation with overlap before Binance's 24 h cut.
- A receive timestamp per message.
- Callbacks that never block. The holder's output ring blocks when full.

**Header:** `src/api/raw/rawWebsocket.hpp`. It's also fine to include it from `api/AstraConnect.hpp`.

```cpp
namespace AstraConnect {

struct RawStreamHandlers {
  // Every text message. recvNs = AstraLib::Time::unixTimestampNS() taken right
  // after the read completed. The view is valid only during the call.
  std::function<void(std::string_view message, uint64_t recvNs)> onMessage;
  // The connection ended for any reason other than close(): server close frame
  // (include code + reason), read/write error, idle timeout. Called exactly once.
  std::function<void(const std::string& reason)> onClose;
};

class RawWebsocket {
 public:
  // Resolves, connects, TLS (verify_peer + host_name_verification + SNI, as in
  // WebsocketStreamHolder::setupConnection), websocket handshake on `path`
  // (e.g. "/public/stream"). Blocks until done; handshake timeout 10 s.
  // On failure returns nullptr and fills `error`. Uses manager.ctx (TLS) only;
  // no credentials needed.
  static std::unique_ptr<RawWebsocket> connect(APIManager& manager, const std::string& host,
                                               const std::string& path, RawStreamHandlers handlers,
                                               FetchError& error);
  // Thread-safe. Queues one text frame. False once the connection is closed.
  bool sendText(const std::string& text);
  // Idempotent. Sends a close frame (best effort, <= 1 s), stops the I/O thread
  // and joins it. After it returns, no callback is running or will run.
  void close();
  ~RawWebsocket();   // = close()
};

}  // namespace AstraConnect
```

**Required behaviour:**

| Topic | Requirement |
|---|---|
| Threads | One I/O thread per connection, started by `connect`. All callbacks run on it, one at a time. |
| Concurrency | Reads and writes must not race on the TLS stream: OpenSSL `SSL*` isn't safe for concurrent `SSL_read`/`SSL_write` from two threads. Use `async_read` / `async_write` on one `io_context` driven by the I/O thread; `sendText` does `net::post` onto it, with a write queue so only one `async_write` is outstanding. |
| Pings | Answer server pings automatically. Binance pings every 3 min and drops the connection if no pong arrives within 10 min. Beast does this by default during reads; don't install a `control_callback` that swallows them. |
| Dead sockets | `websocket::stream_base::timeout{ handshake = 10 s, idle_timeout = 60 s, keep_alive_pings = true }`. Binance allows unsolicited pongs, so our keep-alive pings are fine. An idle timeout ends with `onClose("idle timeout")`. |
| Reconnect | **None.** The recorder logs each disconnect and reconnects with its own backoff. |
| Message size | `read_message_max` ≥ 4 MB (the all-market mark price array is about 150 KB today). |
| Callbacks | `onMessage` must be cheap. The recorder parses there and pushes into a ring with `tryEnqueue`; it never blocks. |
| close() from a callback | Not needed by the recorder. If it's called, don't deadlock (detect the I/O thread and skip the join). |

**Tests to add:**
- Connect to `fstream.binance.com/public/stream`, send `{"method":"SUBSCRIBE","params":["btcusdt@bookTicker"],"id":1}`, and expect `{"result":null,"id":1}` followed by data.
- Call `sendText` from two threads while messages stream in, with no TSAN reports.
- `close()` returns in under 1 s, and no callback arrives after it.
- Cutting the network (e.g. an iptables drop) fires `onClose` within about 70 s.

## B3. `Futures::API::fetchRaw`: raw REST GET

**Why.** The recorder needs the full `exchangeInfo` with every symbol and the
exact decimal **strings** for `tickSize`/`stepSize`. `PairInfo` holds doubles for
one pair. It also needs `/fapi/v1/time` and `/fapi/v1/ticker/24hr`, and it must
see HTTP errors such as 418 and 429.

```cpp
// In APIManager::Futures::API (host fapi.binance.com).
// SUCCESS for any HTTP response, including 4xx/5xx: body and status are filled.
// BOOST_ERROR (with message) if no response could be obtained.
FetchError fetchRaw(const std::string& target, std::string& body, int& httpStatus);
```

**Requirements:**
- **Stale keep-alive connections.** Binance closes idle keep-alive connections, and the recorder calls this every 10 min (clock) and every hour (exchangeInfo). On a write or read error, reconnect once (`retryConnection(false)`) and retry the request once before returning BOOST_ERROR. Today `sendRequest` marks the connection failed and every later call fails until someone calls `retryConnection`.
- **Body limit.** At least 16 MB. exchangeInfo is about 1 MB today, and Beast's response parser defaults to an 8 MB body limit.
- **HTTP status.** Return `res.result_int()`. `sendRequest` currently ignores the status, so a 429 or 418 body is parsed as if it were data.

## B4. Existing streams use the legacy unrouted URLs (bug)

Since 2026-04-23, Binance routes USD-M futures websocket traffic by path:

| Path | Streams |
|---|---|
| `/public` | `<sym>@bookTicker`, `!bookTicker`, `<sym>@depth…` |
| `/market` | `<sym>@aggTrade`, `@markPrice`, `!markPrice@arr`, `@kline_*`, `@ticker`, `@forceOrder`, `!forceOrder@arr`, … |
| `/private` | listenKey user data |

An unrouted connection (`/ws/...`, `/stream?...`) only receives `/public`
streams. I checked this live on 2026-10-12: `wss://fstream.binance.com/ws/btcusdt@aggTrade`
connects but delivers nothing, while `/market/ws/btcusdt@aggTrade` works.

So in AstraConnect today:
- `MarkPriceStream` (`"/ws/" + pair + "@markPrice@1s"`) connects fine but stays silent. `@markPrice` is listed under `/market`.
- `TradeEventStream` uses `"/ws/" + pair + "@trade"`. Binance's mapping excerpt doesn't list `@trade`, so check which path it belongs to. If it's `/market`, it's silent too.
- Check the user data stream (`userFuturesStream.hpp`) against `/private`.

**Fix:** prefix the targets with `/market` or `/public` (`/private` for user data).

Other things worth knowing from the same check:
- `!bookTicker` (all-market) now updates **every 5 s**. It's real-time only per symbol (`<sym>@bookTicker`).
- A single connection may carry up to **1024 streams**, with **10 incoming messages/s** (SUBSCRIBE etc.). Exceeding that disconnects, and repeated disconnects can ban the IP.
- SUBSCRIBE for a nonexistent symbol still answers `{"result":null}`, so success doesn't prove data will come.
- Payloads now carry `st` (1 = UM, 2 = CM) and `ps`, and `aggTrade` has `nq`. The all-market arrays also include `TRADIFI_PERPETUAL` symbols.

## B5. Small fixes

- `api/common/error/fetchError.hpp`: `getErrorFromSimdjson` is a non-`inline` function defined in a header. Including it from two translation units fails to link with multiple definitions. Mark it `inline`. The recorder includes AstraConnect from a single `.cpp`, so it isn't affected.
- `StreamHolder::sendRequest` ignores the HTTP status (see B3).
- Optional: add jitter to `BackoffPolicy`. Many clients reconnecting on the same schedule hit the server together. The recorder uses its own jittered backoff, so this isn't needed for it.

---

## Checking the result

1. Build with:
   ```bash
   cmake -S . -B build -DHAR_BUILD_LIVE=ON -DRECORDER_BUILD_LIVE=ON
   cmake --build build -j
   ```
   This compiles `recorder/src/queue_sink.cpp`, `recorder/src/astra_transport.cpp` and `recorder/apps/recorder_main.cpp` against your changes.
2. Run `ctest --test-dir build` to confirm the offline recorder tests still pass.
3. Do a short live smoke run: `./build/recorder build/recorder.json` with `root` pointed at a scratch folder.
   - After about a minute, `heartbeat.json` should show every group with `msgs_per_s > 0` and `conns_up == conns_total`.
   - Stop it with Ctrl-C. No `.part` files should remain.
   - `python3 recorder/python/acrec_read.py <day>/*.bin.zst` prints counts only.
