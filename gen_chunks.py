"""Generate chunked URL lists for parallel GitHub Actions fetch."""
import json, os
from datetime import date, timedelta
from pathlib import Path

SYMBOLS = {
    "ADAUSD":"ADAUSD","AUDCHF":"AUDCHF","AUDJPY":"AUDJPY","AUDNZD":"AUDNZD","AUDUSD":"AUDUSD",
    "AUS200":"AUSIDXAUD","AVGO.NAS":"AVGOUSUSD","BABA.NYSE":"BABAUSUSD","BTCUSD":"BTCUSD",
    "CADCHF":"CADCHF","CAT.NYSE":"CATUSUSD","CHFJPY":"CHFJPY","CHINA50":"CHIIDXUSD",
    "CVX.NYSE":"CVXUSUSD","ES35":"ESPIDXEUR","ETHUSD":"ETHUSD","EURAUD":"EURAUD",
    "EURCAD":"EURCAD","EURCHF":"EURCHF","EURGBP":"EURGBP","EURJPY":"EURJPY","EURNZD":"EURNZD",
    "EURUSD":"EURUSD","GBPCAD":"GBPCAD","GBPCHF":"GBPCHF","GBPJPY":"GBPJPY","GBPNZD":"GBPNZD",
    "GBPUSD":"GBPUSD","GDX.NYSE":"GDXUSUSD","GDXJ.NYSE":"GDXJUSUSD","IT40":"ITAIDXEUR",
    "IWM.NYSE":"IWMUSUSD","JP225":"JPNIDXJPY","JPM.NYSE":"JPMUSUSD","MU.NAS":"MUUSUSD",
    "NETH25":"NLDIDXEUR","NFLX.NAS":"NFLXUSUSD","NVDA.NAS":"NVDAUSUSD","NZDJPY":"NZDJPY",
    "NZDUSD":"NZDUSD","ORCL.NYSE":"ORCLUSUSD","PG.NYSE":"PGUSUSD","SNAP.NYSE":"SNAPUSUSD",
    "SPY.NYSE":"SPYUSUSD","STOXX50":"EUSIDXEUR","SWI20":"CHEIDXCHF","UBER.NYSE":"UBERUSUSD",
    "UK100":"GBRIDXGBP","US30":"USA30IDXUSD","US500":"USA500IDXUSD","USDCAD":"USDCAD",
    "USDCHF":"USDCHF","USDCNH":"USDCNH","USDJPY":"USDJPY","USDMXN":"USDMXN","USDTRY":"USDTRY",
    "USO.NYSE":"USOUSUSD","V.NYSE":"VUSUSD","VEA.NYSE":"VEAUSUSD","XAGUSD":"XAGUSD",
    "XAUEUR":"XAUEUR","XAUUSD":"XAUUSD","XBRUSD":"BRENTCMDUSD","XLE.NYSE":"XLEUSUSD",
    "XLV.NYSE":"XLVUSUSD","XNGUSD":"GASCMDUSD","XOP.NYSE":"XOPUSUSD",
    "ASML.AMS":"ASMLNLEUR","XAGAUD":"XAGAUD","XAUAUD":"XAUAUD","BNBUSD":"BNBUSD",
}
CRYPTO = {"ADAUSD","BTCUSD","ETHUSD","BNBUSD"}
START = date(2020, 4, 25)
END = date(2026, 4, 25)
N_CHUNKS = 100

def gen_all_tasks():
    tasks = []
    for sym, code in SYMBOLS.items():
        d = START
        while d <= END:
            if not (sym not in CRYPTO and d.weekday() == 5):
                for h in range(24):
                    tasks.append([sym, code, d.year, d.month-1, d.day, h])
            d += timedelta(days=1)
    return tasks

def main():
    tasks = gen_all_tasks()
    n = len(tasks)
    print(f"Total tasks: {n}")
    out_dir = Path(__file__).parent / "chunks"
    out_dir.mkdir(exist_ok=True)
    chunk_size = (n + N_CHUNKS - 1) // N_CHUNKS
    for i in range(N_CHUNKS):
        chunk = tasks[i*chunk_size : (i+1)*chunk_size]
        with open(out_dir / f"chunk_{i:03d}.json", "w") as f:
            json.dump(chunk, f)
    print(f"Wrote {N_CHUNKS} chunks of ~{chunk_size} tasks each to chunks/")

if __name__ == "__main__":
    main()
