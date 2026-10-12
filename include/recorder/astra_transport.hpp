// Transport implemented on AstraConnect (live builds only). The AstraConnect
// headers stay out of this header, so only astra_transport.cpp needs C++20 +
// Boost + OpenSSL.
#pragma once
#include <memory>

#include "recorder/transport.hpp"

namespace acrec {

class AstraTransport : public Transport {
 public:
  AstraTransport();
  ~AstraTransport() override;
  std::unique_ptr<WsConnection> connect(const std::string& host, const std::string& path, WsHandlers handlers,
                                        std::string& err) override;
  bool http_get(const std::string& host, const std::string& target, std::string& body, int& status,
                std::string& err) override;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace acrec
