#include "recorder/parser.hpp"

#include <cstring>

#include "recorder/decimal.hpp"

namespace acrec {
namespace {

namespace od = simdjson::ondemand;

struct BadValue {
  std::string what;
};

int64_t fixed(od::value v, int digits, const char* field) {
  std::string_view s = v.get_string();
  int64_t out;
  // The value itself is left out: the recorder never prints prices.
  if (!parse_fixed(s, digits, out))
    throw BadValue{std::string(field) + " is not an exact decimal with <= " + std::to_string(digits) + " places (" +
                   std::to_string(s.size()) + " chars)"};
  return out;
}
int64_t px(od::object& o, const char* field) { return fixed(o[field], kPriceDigits, field); }

uint8_t liq_status(std::string_view s) {
  for (uint8_t i = 0; i < 5; ++i)
    if (s == kLiqStatusNames[i]) return i;
  return static_cast<uint8_t>(LiqStatus::Unknown);
}

}  // namespace

MessageParser::MessageParser(const SymbolRegistry& registry) : registry_(registry) {}

ParseResult MessageParser::parse(std::string_view msg, uint64_t recv_ns, uint32_t conn_id, const Emit& emit) {
  ParseResult res;
  if (registry_.version() != snap_version_) {
    snap_version_ = registry_.version();
    snap_ = registry_.snapshot();
  }
  // simdjson needs SIMDJSON_PADDING readable bytes past the end.
  buf_.reserve(msg.size() + simdjson::SIMDJSON_PADDING);
  buf_.assign(msg.data(), msg.size());

  // Looks up the symbol; returns false (and counts a skip) outside the universe.
  auto symbol_key = [&](std::string_view s, uint32_t& key) {
    const uint32_t* k = snap_->find_trading(s);
    if (!k) { ++res.skipped; return false; }
    key = *k;
    return true;
  };

  auto handle_event = [&](od::object obj) {
    std::string_view e = obj["e"];
    AnyRecord r;
    r.conn_id = conn_id;
    if (e == "bookTicker") {
      std::string_view s = obj["s"];
      uint32_t key;
      if (!symbol_key(s, key)) return;
      r.group = Group::Bbo;
      BboRecord& b = r.bbo;
      b = BboRecord{};
      b.upd_id = obj["u"];
      b.bid_px = px(obj, "b"); b.bid_qty = px(obj, "B");
      b.ask_px = px(obj, "a"); b.ask_qty = px(obj, "A");
      b.T_ms = obj["T"]; b.E_ms = obj["E"];
      b.recv_ns = recv_ns; b.sym = key;
      r.E_ms = b.E_ms;
    } else if (e == "aggTrade") {
      std::string_view s = obj["s"];
      uint32_t key;
      if (!symbol_key(s, key)) return;
      r.group = Group::Trades;
      TradeRecord& t = r.trade;
      t = TradeRecord{};
      t.E_ms = obj["E"]; t.agg_id = obj["a"];
      t.px = px(obj, "p"); t.qty = px(obj, "q");
      t.first_id = obj["f"]; t.last_id = obj["l"]; t.T_ms = obj["T"];
      t.buyer_is_maker = bool(obj["m"]) ? 1 : 0;
      t.recv_ns = recv_ns; t.sym = key;
      r.E_ms = t.E_ms;
    } else if (e == "depthUpdate") {
      uint64_t E = obj["E"], T = obj["T"];
      std::string_view s = obj["s"];
      uint32_t key;
      if (!symbol_key(s, key)) return;
      r.group = Group::Depth;
      DepthRecord& d = r.depth;
      std::memset(&d, 0, sizeof d);
      d.E_ms = E; d.T_ms = T;
      d.U = obj["U"]; d.u = obj["u"]; d.pu = obj["pu"];
      auto levels = [&](const char* side, int64_t* pxs, int64_t* qtys, uint8_t& n) {
        n = 0;
        for (auto lvl : obj[side].get_array()) {
          if (n == kDepthLevels) throw BadValue{std::string(side) + " has more than 20 levels"};
          od::array pq = lvl.get_array();
          auto it = pq.begin();
          pxs[n] = fixed(*it, kPriceDigits, side);
          ++it;
          qtys[n] = fixed(*it, kPriceDigits, side);
          ++n;
        }
      };
      levels("b", d.bid_px, d.bid_qty, d.n_bid);
      levels("a", d.ask_px, d.ask_qty, d.n_ask);
      d.recv_ns = recv_ns; d.sym = key;
      r.E_ms = d.E_ms;
    } else if (e == "forceOrder") {
      uint64_t E = obj["E"];
      od::object o = obj["o"];
      std::string_view s = o["s"];
      uint32_t key;
      if (!symbol_key(s, key)) return;
      r.group = Group::Liq;
      LiqRecord& l = r.liq;
      l = LiqRecord{};
      l.E_ms = E;
      std::string_view side = o["S"];
      l.side = side == "SELL" ? 1 : 0;
      l.qty = px(o, "q"); l.px = px(o, "p"); l.avg_px = px(o, "ap");
      std::string_view status = o["X"];
      l.status = liq_status(status);
      l.filled_qty = px(o, "z");
      l.T_ms = o["T"];
      l.recv_ns = recv_ns; l.sym = key;
      r.E_ms = l.E_ms;
    } else if (e == "markPriceUpdate") {
      uint64_t E = obj["E"];
      std::string_view s = obj["s"];
      uint32_t key;
      if (!symbol_key(s, key)) return;
      r.group = Group::Mark;
      MarkRecord& m = r.mark;
      m = MarkRecord{};
      m.E_ms = E;
      m.mark_px = px(obj, "p");
      m.index_px = px(obj, "i");
      m.funding_rate_e9 = fixed(obj["r"], kFundingDigits, "r");
      m.next_funding_ms = obj["T"];
      m.recv_ns = recv_ns; m.sym = key;
      r.E_ms = m.E_ms;
    } else {
      throw BadValue{"unknown event type '" + std::string(e) + "'"};
    }
    emit(r);
    ++res.records;
  };

  try {
    od::document doc = parser_.iterate(simdjson::padded_string_view(buf_.data(), buf_.size(), buf_.capacity()));
    od::object top = doc.get_object();
    for (auto field : top) {
      std::string_view k = field.unescaped_key();
      if (k == "data") {
        od::value data = field.value();
        if (data.type() == od::json_type::array) {
          for (auto item : data.get_array()) {
            // One bad item in an all-market array must not drop the others.
            try { handle_event(item.get_object()); }
            catch (const BadValue& b) { if (res.error.empty()) res.error = b.what; }
          }
        } else {
          handle_event(data.get_object());
        }
      } else if (k == "result") {
        res.kind = ParseResult::Kind::Reply;
      } else if (k == "id") {
        res.reply_id = field.value().get_int64();
      } else if (k == "error") {
        res.kind = ParseResult::Kind::Error;
        res.error = std::string(std::string_view(field.value().raw_json()));
      }
    }
  } catch (const BadValue& b) {
    res.kind = ParseResult::Kind::Error;
    res.error = b.what;
  } catch (const simdjson::simdjson_error& e) {
    res.kind = ParseResult::Kind::Error;
    res.error = std::string("json: ") + e.what();
  }
  return res;
}

}  // namespace acrec
