#include "recorder/symbols.hpp"

#include <algorithm>
#include <map>

#include <simdjson/simdjson.h>

namespace acrec {

const char* change_kind_name(SymbolChange::Kind k) {
  switch (k) {
    case SymbolChange::Kind::Listing: return "listing";
    case SymbolChange::Kind::Delisting: return "delisting";
    case SymbolChange::Kind::TickChange: return "tick_change";
  }
  return "?";
}

bool parse_exchange_info(std::string_view json, const UniverseFilter& filter, std::vector<SymbolSpec>& out,
                         std::string& err) {
  out.clear();
  simdjson::dom::parser parser;
  simdjson::dom::element doc;
  if (auto e = parser.parse(json.data(), json.size()).get(doc)) { err = std::string("exchangeInfo: ") + simdjson::error_message(e); return false; }
  simdjson::dom::array symbols;
  if (doc["symbols"].get_array().get(symbols)) { err = "exchangeInfo: no symbols array"; return false; }
  auto in = [](const std::vector<std::string>& v, std::string_view s) { return std::find(v.begin(), v.end(), s) != v.end(); };
  for (simdjson::dom::element s : symbols) {
    std::string_view sym, ctype, quote, status;
    if (s["symbol"].get_string().get(sym) || s["contractType"].get_string().get(ctype) ||
        s["quoteAsset"].get_string().get(quote) || s["status"].get_string().get(status)) {
      err = "exchangeInfo: symbol entry missing symbol/contractType/quoteAsset/status";
      return false;
    }
    if (!in(filter.contract_types, ctype) || !in(filter.quote_assets, quote)) continue;
    SymbolSpec spec;
    spec.symbol = sym; spec.contract_type = ctype; spec.quote = quote; spec.status = status;
    simdjson::dom::array filters;
    if (s["filters"].get_array().get(filters)) { err = "exchangeInfo: " + spec.symbol + " has no filters"; return false; }
    for (simdjson::dom::element f : filters) {
      std::string_view type, v;
      if (f["filterType"].get_string().get(type)) continue;
      if (type == "PRICE_FILTER" && !f["tickSize"].get_string().get(v)) spec.tick_size = v;
      if (type == "LOT_SIZE" && !f["stepSize"].get_string().get(v)) spec.step_size = v;
    }
    if (spec.tick_size.empty() || spec.step_size.empty()) { err = "exchangeInfo: " + spec.symbol + " lacks tickSize/stepSize"; return false; }
    out.push_back(std::move(spec));
  }
  return true;
}

const uint32_t* SymbolRegistry::Snapshot::find_trading(std::string_view symbol) const {
  auto it = trading.find(std::string(symbol));
  return it == trading.end() ? nullptr : &it->second;
}

SymbolRegistry::SymbolRegistry() : snap_(std::make_shared<Snapshot>()) {}

std::shared_ptr<const SymbolRegistry::Snapshot> SymbolRegistry::snapshot() const {
  std::lock_guard<std::mutex> g(mu_);
  return snap_;
}

uint32_t SymbolRegistry::intern(const SymbolSpec& spec) {
  std::lock_guard<std::mutex> g(mu_);
  for (uint32_t k = 0; k < snap_->specs.size(); ++k) {
    const SymbolSpec& s = snap_->specs[k];
    if (s.symbol == spec.symbol && s.tick_size == spec.tick_size && s.step_size == spec.step_size) return k;
  }
  auto next = std::make_shared<Snapshot>(*snap_);
  const uint32_t key = static_cast<uint32_t>(next->specs.size());
  next->specs.push_back(spec);
  if (!next->latest.count(spec.symbol)) next->latest[spec.symbol] = key;
  snap_ = std::move(next);
  version_.fetch_add(1, std::memory_order_acq_rel);
  return key;
}

std::vector<SymbolChange> SymbolRegistry::update(const std::vector<SymbolSpec>& specs, int64_t now_ms) {
  std::lock_guard<std::mutex> g(mu_);
  auto next = std::make_shared<Snapshot>(*snap_);
  std::vector<SymbolChange> changes;
  std::map<std::string, const SymbolSpec*> incoming;
  for (const SymbolSpec& s : specs) incoming[s.symbol] = &s;

  for (const auto& [name, s] : incoming) {
    const bool trading = s->status == "TRADING";
    auto prev = next->latest.find(name);
    uint32_t key;
    if (prev == next->latest.end()) {
      key = static_cast<uint32_t>(next->specs.size());
      next->specs.push_back(*s);
      next->specs.back().added_at_ms = now_ms;
      next->latest[name] = key;
    } else {
      key = prev->second;
      SymbolSpec& old = next->specs[key];
      if (old.tick_size != s->tick_size || old.step_size != s->step_size) {
        const std::string detail = "tick " + old.tick_size + "->" + s->tick_size + ", step " + old.step_size + "->" + s->step_size;
        key = static_cast<uint32_t>(next->specs.size());
        next->specs.push_back(*s);
        next->specs.back().added_at_ms = now_ms;
        next->latest[name] = key;
        if (trading && next->trading.count(name)) changes.push_back({SymbolChange::Kind::TickChange, name, detail});
      } else {
        old.status = s->status;
      }
    }
    const bool was_trading = next->trading.count(name) > 0;
    if (trading) {
      next->trading[name] = key;
      if (!was_trading) changes.push_back({SymbolChange::Kind::Listing, name, "status " + s->status});
    } else if (was_trading) {
      next->trading.erase(name);
      changes.push_back({SymbolChange::Kind::Delisting, name, "status " + s->status});
    }
  }
  // Symbols that vanished from exchangeInfo altogether.
  for (auto it = next->trading.begin(); it != next->trading.end();) {
    if (!incoming.count(it->first)) {
      next->specs[it->second].status = "REMOVED";
      changes.push_back({SymbolChange::Kind::Delisting, it->first, "removed from exchangeInfo"});
      it = next->trading.erase(it);
    } else {
      ++it;
    }
  }
  snap_ = std::move(next);
  version_.fetch_add(1, std::memory_order_acq_rel);
  return changes;
}

}  // namespace acrec
