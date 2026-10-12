// Websocket JSON -> records. One parser per connection (not thread-safe).
#pragma once
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>

#include <simdjson/simdjson.h>

#include "recorder/records.hpp"
#include "recorder/symbols.hpp"

namespace acrec {

struct ParseResult {
  enum class Kind {
    Data,      // zero or more records emitted (array payloads may hold many)
    Reply,     // {"result":..,"id":..} answer to SUBSCRIBE/UNSUBSCRIBE
    Error,     // {"error":..} from the server, or a message we can't parse
  };
  Kind kind = Kind::Data;
  int records = 0;           // emitted
  int skipped = 0;           // symbols outside the universe (other contract types, CM)
  int64_t reply_id = -1;
  // Set for Error, and also for Data when one item of an array payload failed
  // (the other items are still emitted). Never contains prices.
  std::string error;
};

class MessageParser {
 public:
  using Emit = std::function<void(AnyRecord&)>;
  explicit MessageParser(const SymbolRegistry& registry);

  // Parses one text message received at recv_ns on connection conn_id.
  ParseResult parse(std::string_view msg, uint64_t recv_ns, uint32_t conn_id, const Emit& emit);

 private:
  const SymbolRegistry& registry_;
  std::shared_ptr<const SymbolRegistry::Snapshot> snap_;
  uint64_t snap_version_ = ~0ull;
  simdjson::ondemand::parser parser_;
  std::string buf_;
};

}  // namespace acrec
