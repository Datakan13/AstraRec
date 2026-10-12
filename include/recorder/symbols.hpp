// Symbol universe from /fapi/v1/exchangeInfo.
//
// Every version of a symbol's contract specs (tick or step size) gets its own
// global key, so a mid-day tick change becomes a new symbol id in meta.json.
// Network threads look symbols up through an immutable snapshot that is
// swapped when exchangeInfo changes.
#pragma once
#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace acrec {

struct SymbolSpec {
  std::string symbol, tick_size, step_size, quote, contract_type, status;
  int64_t added_at_ms = 0;     // when this version was first seen
};

struct UniverseFilter {
  std::vector<std::string> contract_types{"PERPETUAL"};
  std::vector<std::string> quote_assets{"USDT", "USDC"};
};

// Parses exchangeInfo into specs for every symbol whose contract type and quote
// asset pass the filter (any status). False on malformed JSON.
bool parse_exchange_info(std::string_view json, const UniverseFilter& filter, std::vector<SymbolSpec>& out,
                         std::string& err);

struct SymbolChange {
  enum class Kind { Listing, Delisting, TickChange };
  Kind kind;
  std::string symbol, detail;
};
const char* change_kind_name(SymbolChange::Kind k);

class SymbolRegistry {
 public:
  struct Snapshot {
    std::vector<SymbolSpec> specs;                       // index = global key; every version ever seen
    std::unordered_map<std::string, uint32_t> trading;   // symbol -> key of its current version, status TRADING
    std::unordered_map<std::string, uint32_t> latest;    // symbol -> key of its current version, any status
    const uint32_t* find_trading(std::string_view symbol) const;
  };

  SymbolRegistry();

  // Applies a fresh exchangeInfo result and returns what changed. The first call
  // reports every TRADING symbol as a listing.
  std::vector<SymbolChange> update(const std::vector<SymbolSpec>& specs, int64_t now_ms);

  // Returns the key of this exact version (symbol + tick + step), adding it as a
  // non-trading historical version if unknown. Used to reload a day's meta.json
  // after a restart so ids stay stable within the day.
  uint32_t intern(const SymbolSpec& spec);

  std::shared_ptr<const Snapshot> snapshot() const;
  // Bumped on every update that changes the snapshot; lets readers cache it.
  uint64_t version() const { return version_.load(std::memory_order_acquire); }

 private:
  mutable std::mutex mu_;
  std::shared_ptr<const Snapshot> snap_;
  std::atomic<uint64_t> version_{0};
};

}  // namespace acrec
