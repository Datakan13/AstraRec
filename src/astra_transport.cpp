// Transport on AstraConnect. Uses the additions specified in
// ASTRACONNECT_SPEC.md: AstraConnect::RawWebsocket (B2) and
// Futures::API::fetchRaw (B3). No API keys: everything recorded is public.
#include "recorder/astra_transport.hpp"

#include <api/AstraConnect.hpp>
#include <api/raw/rawWebsocket.hpp>

namespace acrec {
namespace {

class AstraWs : public WsConnection {
 public:
  explicit AstraWs(std::unique_ptr<AstraConnect::RawWebsocket> ws) : ws_(std::move(ws)) {}
  bool send_text(const std::string& text) override { return ws_->sendText(text); }
  void close() override { ws_->close(); }

 private:
  std::unique_ptr<AstraConnect::RawWebsocket> ws_;
};

}  // namespace

struct AstraTransport::Impl {
  APIManager manager;   // market data only: no Credentials
  APIManager::Futures::API rest{manager};
};

AstraTransport::AstraTransport() : impl_(std::make_unique<Impl>()) {}
AstraTransport::~AstraTransport() = default;

std::unique_ptr<WsConnection> AstraTransport::connect(const std::string& host, const std::string& path, WsHandlers h,
                                                      std::string& err) {
  AstraConnect::RawStreamHandlers rh;
  rh.onMessage = std::move(h.on_message);
  rh.onClose = std::move(h.on_close);
  FetchError fe(APIError::SUCCESS);
  auto ws = AstraConnect::RawWebsocket::connect(impl_->manager, host, path, std::move(rh), fe);
  if (!ws) {
    err = fe.getErrorMsg();
    return nullptr;
  }
  return std::make_unique<AstraWs>(std::move(ws));
}

bool AstraTransport::http_get(const std::string& host, const std::string& target, std::string& body, int& status,
                              std::string& err) {
  if (host != impl_->manager.hostFutures) {
    err = "AstraTransport only serves " + impl_->manager.hostFutures;
    return false;
  }
  FetchError fe = impl_->rest.fetchRaw(target, body, status);
  if (fe) {
    err = fe.getErrorMsg();
    return false;
  }
  return true;
}

}  // namespace acrec
