"""Full pre-registered tournament: grid -> rotating-block CV -> permutation MC -> sealed holdout.

Usage: python scripts/tourney_full.py <cand_parquet> <out_dir> [interval]

Protocol is fixed in docs/PREREG_tournament.md and must not be edited after seeing output.
"""
import itertools
import json
import os
import sys
import time

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
import tourney_engine as T

K = 5
N_PERM = 300
HOLD_FRAC = 0.20
MIN_POS_BLOCKS = 4

DEPLOY = {
    "5m": dict(disp_floor=2.0, vol_floor=0.12, z_min=0.45, edge_min=0.02, min_ask=0.30,
               max_ask=1.0, min_ttl=30, max_ttl=240, frozen=2, vol_lb=60,
               intervals=["5m"], mid_move_max=0.0, max_bbo_age=2, burst_min=0),
    "15m": dict(disp_floor=2.0, vol_floor=0.07, z_min=0.70, edge_min=0.06, min_ask=0.30,
                max_ask=0.70, min_ttl=30, max_ttl=540, frozen=1e9, vol_lb=120,
                intervals=["15m"], mid_move_max=0.0, max_bbo_age=2, burst_min=0),
}


def build_grid(iv):
    base = DEPLOY[iv]
    cfgs, seen = [], set()

    def add(c, tag):
        k = json.dumps(c, sort_keys=True, default=str)
        if k not in seen:
            seen.add(k)
            cfgs.append((c, tag))

    def burst_arm(bm, mx, zm=-99):
        c = dict(base)
        c.update(burst_min=bm, mid_move_max=None, frozen=1e9, z_min=zm,
                 edge_min=-99, disp_floor=0.0, vol_floor=0.0, max_ask=mx)
        return c

    add(dict(base), "DEPLOYED")
    axes = dict(
        disp_floor=[1.5, 2.0, 3.0, 4.0],
        vol_floor=[0.06, base["vol_floor"], 0.18, 0.25],
        z_min=[0.35, base["z_min"], 0.80, 1.10],
        edge_min=[0.0, base["edge_min"], 0.04, 0.06],
        max_ttl=[150, base["max_ttl"], (270 if iv == "5m" else 700)],
        min_ask=[0.30, 0.45, 0.55],
        max_ask=[base["max_ask"], 0.85, 0.70],
        vol_lb=[60, 120],
        mid_move_max=[0.0, None],
        frozen=[2, 1e9],
    )
    for k, vals in axes.items():                                  # Stage A
        for v in vals:
            c = dict(base); c[k] = v
            add(c, f"A:{k}={v}")
    for dz, zm, em, vlb, mm in itertools.product(                 # Stage B
            axes["disp_floor"], axes["z_min"], axes["edge_min"],
            axes["vol_lb"], axes["mid_move_max"]):
        c = dict(base)
        c.update(disp_floor=dz, z_min=zm, edge_min=em, vol_lb=vlb, mid_move_max=mm)
        add(c, "B")
    for bm, mx in itertools.product([2, 3, 5, 8], [0.60, 0.65, 0.75, 1.0]):   # Stage C
        add(burst_arm(bm, mx), f"C:burst>={bm},ask<={mx}")
    for bm, mx in itertools.product([2, 3, 5, 8], [0.60, 0.65, 0.75, 1.0]):   # Stage D
        u = dict(base); u["union"] = [dict(base), burst_arm(bm, mx)]
        add(u, f"D:union burst>={bm},ask<={mx}")
    return cfgs


def blocks(days, k):
    return [list(x) for x in np.array_split(np.array(days), k)]


def cv_procedure(M, dev_days):
    bl = blocks(dev_days, K)
    out = []
    for f in range(K):
        te = [f"d_{x}" for x in bl[f] if f"d_{x}" in M.columns]
        trb = [b for j, b in enumerate(bl) if j != f]
        tr = [f"d_{x}" for b in trb for x in b if f"d_{x}" in M.columns]
        if not te or not tr:
            continue
        ism = M[tr].mean(axis=1)
        bm = pd.concat([M[[f"d_{x}" for x in b if f"d_{x}" in M.columns]].mean(axis=1)
                        for b in trb], axis=1)
        elig = ism[(bm > 0).sum(axis=1) >= min(MIN_POS_BLOCKS, bm.shape[1])]
        if elig.empty:
            elig = ism
        w = elig.idxmax()
        out.append(dict(fold=f, winner=w, is_=float(ism[w]), oos=float(M.loc[w, te].mean())))
    return pd.DataFrame(out)


def main(cand, out_dir, only=None):
    os.makedirs(out_dir, exist_ok=True)
    d = pd.read_parquet(cand)
    stops = None
    rng = np.random.default_rng(20260727)
    report = {}

    for iv in (["5m", "15m"] if only is None else [only]):
        sub = d[d.interval == iv]
        mpd = sub.groupby("day").mkey.nunique()
        thresh = mpd.median() * 0.85
        days = sorted(mpd[mpd >= thresh].index)
        sub = sub[sub.day.isin(days)].reset_index(drop=True)
        nh = max(1, int(round(len(days) * HOLD_FRAC)))
        dev, hold = days[:-nh], days[-nh:]
        cfgs = build_grid(iv)
        print(f"\n{'='*80}\n{iv}: {len(cfgs)} configs | {len(days)} full days "
              f"({days[0]}..{days[-1]}) | dev {len(dev)} | SEALED holdout {len(hold)} "
              f"({hold[0]}..{hold[-1]})\n{'='*80}", flush=True)

        # market -> group for the permutation null
        mk = sub.mkey.to_numpy()
        grp = (sub.day + "|" + sub.asset).to_numpy()
        umk, inv = np.unique(mk, return_inverse=True)
        mk_sign = pd.Series(sub.won.to_numpy() == (sub.side.to_numpy() == "Up")
                            ).groupby(inv).first().to_numpy()   # True => Up won
        mk_grp = pd.Series(grp).groupby(inv).first().to_numpy()

        t0 = time.time()
        rows, sels = [], {}
        for i, (c, tag) in enumerate(cfgs):
            pos = T.select(sub, c)
            sels[i] = pos
            s = T.score(sub, pos, stops, delayed=1, exit_mode="band")
            s2 = T.score(sub, pos, stops, delayed=2, exit_mode="band")
            pd_ = s.groupby("day").pnl.sum() if len(s) else pd.Series(dtype=float)
            rows.append(dict(i=i, tag=tag, n=len(s),
                             wr=s.won.mean() if len(s) else np.nan,
                             pf=s.pf.mean() if len(s) else np.nan,
                             ask=s.ask.mean() if len(s) else np.nan,
                             d2_mean=(s2.pnl.sum() / len(days)) if len(s2) else 0.0,
                             **{f"d_{k}": v for k, v in pd_.items()}))
            if (i + 1) % 100 == 0:
                print(f"   {i+1}/{len(cfgs)} ({time.time()-t0:.0f}s)", flush=True)
        M = pd.DataFrame(rows).set_index("i")
        dcols = [c for c in M.columns if c.startswith("d_")]
        M[dcols] = M[dcols].fillna(0.0)
        M.to_parquet(os.path.join(out_dir, f"grid_{iv}.parquet"))

        dep = int(M.index[M.tag == "DEPLOYED"][0])
        cv = cv_procedure(M, dev)
        dep_oos = float(np.mean([M.loc[dep, [f"d_{x}" for x in b if f"d_{x}" in M.columns]].mean()
                                 for b in blocks(dev, K)]))
        proc = float(cv.oos.mean())
        print("\n-- rotating-block CV --")
        print(cv.assign(tag=[M.loc[w, "tag"] for w in cv.winner]).to_string(index=False))
        print(f"\n  procedure mean OOS $/day {proc:+.3f} | DEPLOYED {dep_oos:+.3f} "
              f"| edge {proc-dep_oos:+.3f} | overfit tax {cv.is_.mean()-proc:+.3f}")

        print(f"\n-- permutation MC ({N_PERM}) --", flush=True)
        nulls = []
        for p in range(N_PERM):
            sgn = mk_sign.copy()
            for g in np.unique(mk_grp):
                m = mk_grp == g
                sgn[m] = rng.permutation(sgn[m])
            won_perm = (sgn[inv] == (sub.side.to_numpy() == "Up")).astype(float)
            cols = {}
            for i in M.index:
                pos = sels[i]
                if len(pos) == 0:
                    cols[i] = pd.Series(dtype=float); continue
                s = T.score(sub, pos, stops, delayed=1, exit_mode="band",
                            won_override=won_perm[pos])
                cols[i] = s.groupby("day").pnl.sum()
            Mn = pd.DataFrame(cols).T
            Mn.columns = [f"d_{c}" for c in Mn.columns]
            Mn = Mn.fillna(0.0)
            nulls.append(float(cv_procedure(Mn, dev).oos.mean()))
            if (p + 1) % 50 == 0:
                print(f"   perm {p+1}/{N_PERM} ({time.time()-t0:.0f}s)", flush=True)
        nulls = np.array(nulls)
        pval = float((nulls >= proc).mean())
        print(f"  null mean {nulls.mean():+.3f} p95 {np.percentile(nulls,95):+.3f} "
              f"max {nulls.max():+.3f} | observed {proc:+.3f} -> p={pval:.4f}")

        isall = M[[f"d_{x}" for x in dev if f"d_{x}" in M.columns]].mean(axis=1)
        bmm = pd.concat([M[[f"d_{x}" for x in b if f"d_{x}" in M.columns]].mean(axis=1)
                         for b in blocks(dev, K)], axis=1)
        elig = isall[(bmm > 0).sum(axis=1) >= MIN_POS_BLOCKS]
        fin = int((elig if not elig.empty else isall).idxmax())
        hc = [f"d_{x}" for x in hold if f"d_{x}" in M.columns]
        hs, hd = float(M.loc[fin, hc].mean()), float(M.loc[dep, hc].mean())
        print(f"\n-- SEALED HOLDOUT (one evaluation) --")
        print(f"  winner #{fin} [{M.loc[fin,'tag']}] n/day={M.loc[fin,'n']/len(days):.1f} "
              f"WR={M.loc[fin,'wr']:.3f} ask={M.loc[fin,'ask']:.3f} pf={M.loc[fin,'pf']:.3f}")
        print(f"  dev $/day: winner {isall[fin]:+.2f} vs deployed {isall[dep]:+.2f}")
        print(f"  HOLDOUT $/day: winner {hs:+.2f} vs deployed {hd:+.2f} -> diff {hs-hd:+.2f}")
        print(f"  2s-latency stress $/day: winner {M.loc[fin,'d2_mean']:+.2f} "
              f"vs deployed {M.loc[dep,'d2_mean']:+.2f}")
        report[iv] = dict(proc_oos=proc, dep_oos=dep_oos, pval=pval, hold_win=hs,
                          hold_dep=hd, winner_tag=M.loc[fin, "tag"],
                          winner_cfg=cfgs[fin][0], n_days=len(days))
    with open(os.path.join(out_dir, "report.json"), "w") as fh:
        json.dump(report, fh, indent=1, default=str)
    print("\nreport ->", os.path.join(out_dir, "report.json"))
    return report


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3] if len(sys.argv) > 3 else None)
