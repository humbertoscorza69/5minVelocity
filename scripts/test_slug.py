import json
import urllib.request

H = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/126",
     "Accept": "application/json"}

for url in [
    "https://gamma-api.polymarket.com/markets?slug=btc-updown-5m-1778803200",
    "https://gamma-api.polymarket.com/markets?slug=btc-updown-15m-1781136000",
]:
    try:
        req = urllib.request.Request(url, headers=H)
        with urllib.request.urlopen(req, timeout=30) as r:
            data = json.load(r)
        if data:
            m = data[0]
            keep = {k: m.get(k) for k in ["slug", "question", "outcomes",
                                          "outcomePrices", "closed",
                                          "umaResolutionStatus", "clobTokenIds",
                                          "conditionId", "endDate"]}
            print(url.split("slug=")[1], "->", json.dumps(keep)[:600])
        else:
            print(url.split("slug=")[1], "-> EMPTY")
    except Exception as e:
        print(url, "ERR", e)
