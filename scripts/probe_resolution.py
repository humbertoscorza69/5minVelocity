import json, time, urllib.request
H = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
def get(u):
    try:
        return json.load(urllib.request.urlopen(urllib.request.Request(u, headers=H), timeout=15))
    except Exception as e:
        return {"_err": str(e)[:60]}
now = int(time.time())
for back in [1, 2, 3, 4, 5]:
    ep = (now // 300) * 300 - back * 300       # window start; settles at ep+300
    ago = now - (ep + 300)
    slug = f"btc-updown-5m-{ep}"
    g = get("https://gamma-api.polymarket.com/markets?slug=" + slug)
    if not isinstance(g, list) or not g:
        print(slug, "gamma none/err", g if isinstance(g, dict) else "")
        continue
    m = g[0]; cid = m.get("conditionId")
    c = get("https://clob.polymarket.com/markets/" + cid)
    wins = [(t.get("outcome"), t.get("winner")) for t in c.get("tokens", [])] if isinstance(c, dict) else "err"
    print(f"{slug} settled {ago}s ago | gamma closed={m.get('closed')} "
          f"outcomePrices={m.get('outcomePrices')} uma={m.get('umaResolutionStatus')} "
          f"| clob winners={wins}")
