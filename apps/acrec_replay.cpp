// acrec_replay: feed websocket messages from files through the recorder's
// parser -> writer -> storage path, with no network. Used by the acceptance
// tests; also handy for re-checking captured traffic.
//
//   acrec_replay --root DIR --exchange-info FILE MESSAGES...
//
// MESSAGES: one websocket text message per line, optionally prefixed with
// "<recv_ns>\t". Each file is treated as one connection (id = file index).
// Gap rows are stamped with the record's exchange time, so a replay is
// deterministic.
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "recorder/parser.hpp"
#include "recorder/storage.hpp"
#include "recorder/symbols.hpp"
#include "recorder/timeutil.hpp"
#include "recorder/writer_core.hpp"

int main(int argc, char** argv) {
  std::string root, info_path;
  std::vector<std::string> inputs;
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a == "--root" && i + 1 < argc) root = argv[++i];
    else if (a == "--exchange-info" && i + 1 < argc) info_path = argv[++i];
    else if (!a.empty() && a[0] == '-') { std::cerr << "unknown option " << a << "\n"; return 2; }
    else inputs.push_back(a);
  }
  if (root.empty() || info_path.empty() || inputs.empty()) {
    std::cerr << "usage: acrec_replay --root DIR --exchange-info FILE MESSAGES...\n";
    return 2;
  }

  std::ifstream info_in(info_path);
  std::stringstream info;
  info << info_in.rdbuf();
  if (!info_in) { std::cerr << "cannot read " << info_path << "\n"; return 2; }
  acrec::SymbolRegistry registry;
  std::vector<acrec::SymbolSpec> specs;
  std::string err;
  if (!acrec::parse_exchange_info(info.str(), acrec::UniverseFilter{}, specs, err)) { std::cerr << err << "\n"; return 2; }
  registry.update(specs, 0);

  acrec::Storage storage({root, 3, "replay"}, registry);
  for (const auto& p : storage.recover()) std::cerr << "recovered " << p << "\n";
  acrec::WriterCore writer(storage, registry, {});

  uint64_t lines = 0, records = 0, skipped = 0, errors = 0;
  for (uint32_t conn = 0; conn < inputs.size(); ++conn) {
    std::ifstream in(inputs[conn]);
    if (!in) { std::cerr << "cannot read " << inputs[conn] << "\n"; return 2; }
    acrec::MessageParser parser(registry);
    std::string line;
    while (std::getline(in, line)) {
      if (line.empty()) continue;
      ++lines;
      uint64_t recv_ns = lines;
      const size_t tab = line.find('\t');
      if (tab != std::string::npos) {
        recv_ns = std::stoull(line.substr(0, tab));
        line.erase(0, tab + 1);
      }
      auto res = parser.parse(line, recv_ns, conn, [&](acrec::AnyRecord& r) {
        writer.record(r, static_cast<int64_t>(r.E_ms));
      });
      records += res.records;
      skipped += res.skipped;
      if (!res.error.empty()) {
        ++errors;
        std::cerr << inputs[conn] << ":" << lines << ": " << res.error << "\n";
      }
    }
  }
  writer.finish();
  std::cout << "lines " << lines << " records " << records << " skipped " << skipped << " errors " << errors
            << " gaps " << writer.stats().gaps << "\n";
  return errors || !storage.last_error().empty() ? 1 : 0;
}
