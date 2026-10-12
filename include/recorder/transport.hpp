// What the recorder needs from the network. The live implementation
// (src/astra_transport.cpp) wraps AstraConnect; tests use a fake. The exact
// AstraConnect additions behind it are in recorder/ASTRACONNECT_SPEC.md.
#pragma once
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>

namespace acrec {

struct WsHandlers {
  // Every text message, on the connection's own thread, with the local
  // CLOCK_REALTIME receive time taken right after the read. Must not block.
  std::function<void(std::string_view msg, uint64_t recv_ns)> on_message;
  // The connection ended for any reason other than close(): server close,
  // network error, read timeout. Called once, on the connection's thread.
  std::function<void(const std::string& reason)> on_close;
};

// One websocket. It never reconnects by itself: the recorder decides when to
// reconnect, so every disconnect can be logged. Server pings are answered
// automatically.
class WsConnection {
 public:
  virtual ~WsConnection() = default;
  // Thread-safe. False if the connection is already closed.
  virtual bool send_text(const std::string& text) = 0;
  // Idempotent. Returns only after the read thread has stopped: no callback
  // runs after close() returns.
  virtual void close() = 0;
};

class Transport {
 public:
  virtual ~Transport() = default;
  // Blocks until the TLS + websocket handshake finished or failed.
  virtual std::unique_ptr<WsConnection> connect(const std::string& host, const std::string& path, WsHandlers handlers,
                                                std::string& err) = 0;
  // HTTPS GET. Returns false on transport errors; HTTP errors return true with status set.
  virtual bool http_get(const std::string& host, const std::string& target, std::string& body, int& status,
                        std::string& err) = 0;
};

}  // namespace acrec
