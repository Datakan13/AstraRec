"""Generates the recorder test fixtures (deterministic, values made up).

Layouts follow Binance's USD-M futures payloads as checked live on 2026-10-12
(combined-stream envelope {"stream", "data"}; fields incl. st/ps/nq). No real
market data is used. Run from this directory: python3 make_fixtures.py
"""
import json
import random
from decimal import Decimal

rng = random.Random(20261101)
DAY0 = 1793491200000  # 2026-11-01T00:00:00Z

SYMBOLS = {  # symbol: (tick, step, quote, start price)
    "BTCUSDT": ("0.10", "0.001", "USDT", Decimal("84000.00")),
    "ETHUSDT": ("0.01", "0.001", "USDT", Decimal("3100.00")),
    "1000PEPEUSDC": ("0.0000001", "1", "USDC", Decimal("0.0123456")),
    "SOLUSDT": ("0.0100", "1", "USDT", Decimal("150.00")),
}


def exchange_info():
    def sym(name, ctype, quote, status, tick, step):
        return {"symbol": name, "pair": name, "contractType": ctype, "status": status, "baseAsset": name[:-4],
                "quoteAsset": quote, "marginAsset": quote, "underlyingType": "COIN",
                "filters": [{"filterType": "PRICE_FILTER", "tickSize": tick, "minPrice": tick, "maxPrice": "1000000"},
                            {"filterType": "LOT_SIZE", "stepSize": step, "minQty": step, "maxQty": "1000"},
                            {"filterType": "MIN_NOTIONAL", "notional": "5"}]}
    syms = [sym(n, "PERPETUAL", q, "TRADING", t, s) for n, (t, s, q, _) in SYMBOLS.items()]
    syms.append(sym("XAUUSDT", "TRADIFI_PERPETUAL", "USDT", "TRADING", "0.01", "0.001"))  # filtered out
    syms.append(sym("OLDUSDT", "PERPETUAL", "USDT", "SETTLING", "0.001", "1"))           # not trading
    syms.append(sym("BTCUSDT_261225", "CURRENT_QUARTER", "USDT", "TRADING", "0.10", "0.001"))
    return {"timezone": "UTC", "serverTime": DAY0, "futuresType": "U_MARGINED", "rateLimits": [],
            "exchangeFilters": [], "assets": [], "symbols": syms}


def q(x, unit):
    unit = Decimal(unit)
    return (Decimal(x) / unit).to_integral_value() * unit


def fmt(d: Decimal) -> str:
    return format(d, "f")


def wrap(stream, data):
    return json.dumps({"stream": stream, "data": data}, separators=(",", ":"))


def main():
    with open("exchange_info.json", "w") as f:
        json.dump(exchange_info(), f, indent=1)

    names = list(SYMBOLS)
    price = {n: SYMBOLS[n][3] for n in names}
    ids = {n: rng.randrange(10**9, 10**10) for n in names}

    def step_price(n):
        tick = SYMBOLS[n][0]
        price[n] = q(price[n] * Decimal(1 + rng.uniform(-0.001, 0.001)), tick)
        return price[n]

    def qty(n):
        step = Decimal(SYMBOLS[n][1])
        return step * rng.randrange(1, 5000)

    # bbo: 50 messages
    lines, t = [], DAY0 + 1000
    upd = {n: rng.randrange(10**12, 10**13) for n in names}
    for i in range(50):
        n = names[i % 4]; t += rng.randrange(1, 50); upd[n] += rng.randrange(1, 9)
        p = step_price(n); tick = Decimal(SYMBOLS[n][0])
        lines.append(wrap(n.lower() + "@bookTicker", {
            "e": "bookTicker", "u": upd[n], "s": n, "ps": n, "b": fmt(p), "B": fmt(qty(n)), "a": fmt(p + tick),
            "A": fmt(qty(n)), "T": t - 2, "E": t, "st": 1}))
    lines.insert(0, json.dumps({"result": None, "id": 1}))  # subscribe reply: not data
    open("bbo.jsonl", "w").write("\n".join(lines) + "\n")

    # trades: 50 messages, agg id +1 per symbol
    lines, t = [], DAY0 + 2000
    for i in range(50):
        n = names[i % 4]; t += rng.randrange(1, 50); ids[n] += 1
        first = ids[n] * 3; q_ = qty(n)
        lines.append(wrap(n.lower() + "@aggTrade", {
            "e": "aggTrade", "E": t, "a": ids[n], "s": n, "p": fmt(step_price(n)), "q": fmt(q_), "nq": fmt(q_),
            "f": first, "l": first + rng.randrange(0, 3), "T": t - 1, "m": rng.random() < 0.5, "st": 1}))
    open("trades.jsonl", "w").write("\n".join(lines) + "\n")

    # depth: 50 messages, pu == previous u per symbol, 20 levels (one with fewer)
    lines, t = [], DAY0 + 3000
    last_u = {n: rng.randrange(10**12, 10**13) for n in names}
    for i in range(50):
        n = names[i % 4]; t += 100
        tick = Decimal(SYMBOLS[n][0]); mid = step_price(n)
        U = last_u[n] + 1; u = U + rng.randrange(0, 40)
        levels = 20 if i != 7 else 13
        bids = [[fmt(mid - tick * (k + 1)), fmt(qty(n))] for k in range(levels)]
        asks = [[fmt(mid + tick * (k + 1)), fmt(qty(n))] for k in range(20)]
        lines.append(wrap(n.lower() + "@depth20@100ms", {
            "e": "depthUpdate", "E": t, "T": t - 3, "s": n, "ps": n, "U": U, "u": u, "pu": last_u[n],
            "b": bids, "a": asks, "st": 1}))
        last_u[n] = u
    open("depth.jsonl", "w").write("\n".join(lines) + "\n")

    # liq: 50 messages (plus one for a symbol outside the universe)
    lines, t = [], DAY0 + 4000
    statuses = ["FILLED", "FILLED", "PARTIALLY_FILLED", "NEW", "EXPIRED"]
    for i in range(50):
        n = names[i % 4]; t += rng.randrange(1, 900)
        p = step_price(n); q_ = qty(n)
        avg = (p * Decimal("1.000123456")).quantize(Decimal("0.00000001"))   # average price: off-tick
        lines.append(wrap("!forceOrder@arr", {"e": "forceOrder", "E": t, "o": {
            "s": n, "S": rng.choice(["BUY", "SELL"]), "o": "LIMIT", "f": "IOC", "q": fmt(q_), "p": fmt(p),
            "ap": fmt(avg), "X": statuses[i % 5], "l": fmt(q_), "z": fmt(q_), "T": t - 1, "ps": n, "st": 1}}))
    lines.append(wrap("!forceOrder@arr", {"e": "forceOrder", "E": t + 1, "o": {
        "s": "XAUUSDT", "S": "SELL", "o": "LIMIT", "f": "IOC", "q": "1", "p": "2000", "ap": "2000", "X": "FILLED",
        "l": "1", "z": "1", "T": t, "ps": "XAUUSDT", "st": 1}}))
    open("liq.jsonl", "w").write("\n".join(lines) + "\n")

    # mark: 13 array messages x 4 symbols = 52 records, plus one out-of-universe item each
    lines, t = [], DAY0 + 5000
    for i in range(13):
        t += 1000
        items = []
        for n in names:
            m = step_price(n) * Decimal("1.0000003")
            items.append({"e": "markPriceUpdate", "E": t, "s": n, "p": fmt(m.quantize(Decimal("0.00000001"))),
                          "ap": fmt(m.quantize(Decimal("0.00000001"))),
                          "P": fmt((m * Decimal("0.99999")).quantize(Decimal("0.00000001"))),
                          "i": fmt((m * Decimal("0.99998")).quantize(Decimal("0.00000001"))),
                          "r": fmt(Decimal(rng.randrange(-30000, 30000)) / Decimal(10**8)),
                          "T": DAY0 + 8 * 3600000, "st": 1})
        items.append({"e": "markPriceUpdate", "E": t, "s": "XAUUSDT", "p": "2000.1", "ap": "2000.1", "P": "2000",
                      "i": "2000", "r": "0.0001", "T": DAY0 + 8 * 3600000, "st": 1})
        lines.append(wrap("!markPrice@arr@1s", items))
    open("mark.jsonl", "w").write("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
