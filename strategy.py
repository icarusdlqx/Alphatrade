from __future__ import annotations
import pandas as pd
import numpy as np

def compute_features(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    if not isinstance(df.index, pd.MultiIndex) or "symbol" not in df.index.names:
        raise ValueError("Bars DataFrame must be multi-indexed by ['symbol','timestamp'].")
    symbols = sorted(set(df.index.get_level_values("symbol")))
    feats = []
    for sym in symbols:
        s = df.xs(sym, level="symbol").sort_index()
        px = s["close"].copy()
        if len(px) < 60:
            continue
        ret_21 = px.pct_change(21).iloc[-1]
        ret_63 = px.pct_change(63).iloc[-1]
        vol_20 = px.pct_change().rolling(20).std().iloc[-1] * np.sqrt(252)
        maxdd = _max_drawdown(px)
        ma20 = px.rolling(20).mean().iloc[-1]
        ma50 = px.rolling(50).mean().iloc[-1]
        trend = (ma20 - ma50) / ma50 if ma50 != 0 else 0.0

        ret_126 = px.pct_change(126).iloc[-1] if len(px) >= 126 else np.nan
        vol_63 = px.pct_change().rolling(63).std().iloc[-1] * np.sqrt(252) if len(px) >= 63 else np.nan
        qual = (ret_126 / vol_63) if (vol_63 and vol_63 != 0 and not np.isnan(vol_63)) else np.nan

        feats.append([sym, float(ret_21), float(ret_63), float(vol_20), float(maxdd), float(trend), float(px.iloc[-1]), 
                      float(ret_126 if ret_126==ret_126 else 0.0), float(vol_63 if vol_63==vol_63 else 0.0), float(qual if qual==qual else 0.0)])
    out = pd.DataFrame(feats, columns=[
        "symbol","ret21","ret63","vol20_annual","maxdd","trend","last","ret126","vol63_annual","qual126"
    ])
    out["score"] = (0.28*out["ret63"] + 0.28*out["ret21"] + 0.28*out["trend"] + 0.16*out["qual126"]) - 0.12*out["vol20_annual"] - 0.08*out["maxdd"].abs()
    out = out.dropna().sort_values("score", ascending=False)
    return out

def compute_breadth(feats: pd.DataFrame) -> float:
    if feats is None or feats.empty:
        return 0.0
    return float((feats["trend"] > 0).mean())

def spy_regime(feats: pd.DataFrame) -> dict:
    spy = feats[feats["symbol"]=="SPY"]
    if spy.empty:
        return {"spy_trend": 0.0, "spy_vol": 0.0}
    row = spy.iloc[0]
    return {"spy_trend": float(row["trend"]), "spy_vol": float(row["vol20_annual"])}

def risk_weights_for(picks, feats: pd.DataFrame, max_weight: float) -> dict:
    f = feats.set_index("symbol")
    raw = {}
    for p in picks:
        sym = p["symbol"]
        if sym not in f.index: 
            continue
        score = max(float(f.at[sym, "score"]), 0.0)
        vol = float(f.at[sym, "vol20_annual"]) if float(f.at[sym, "vol20_annual"])>0 else 0.0001
        raw[sym] = score / vol
    if not raw:
        return {}
    s = sum(raw.values())
    if s == 0: 
        return {}
    w = {k: min(max_weight, v/s) for k,v in raw.items()}
    tw = sum(w.values())
    if tw > 1.0:
        residual = 1.0
        alloc = {}
        for k, v in sorted(w.items(), key=lambda kv: kv[1], reverse=True):
            a = min(v, max_weight, residual)
            alloc[k] = a
            residual -= a
            if residual <= 1e-6:
                break
        w = alloc
    return w

def _max_drawdown(px: pd.Series) -> float:
    roll_max = px.cummax()
    dd = px/roll_max - 1.0
    return float(dd.min())
