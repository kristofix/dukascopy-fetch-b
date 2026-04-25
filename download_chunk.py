"""Download one chunk of Dukascopy hour-files. Runs on GitHub Actions runner.

Output: data/{SYM}/{Y}/{MM}/{DD}/{HH}.bi5 (or .404 sentinel).
After completion, runner packages data/ as tar.gz and uploads to release.
"""
import asyncio, json, os, random, sys, time
from pathlib import Path
import httpx

URL_T = "https://datafeed.dukascopy.com/datafeed/{code}/{y}/{m0:02d}/{d:02d}/{h:02d}h_ticks.bi5"
ROOT = Path(os.environ.get("OUT_DIR", "data"))

async def fetch_one(client, task, retries=5):
    sym, code, y, m0, d, h = task
    out_dir = ROOT / sym / f"{y:04d}" / f"{m0+1:02d}" / f"{d:02d}"
    out_dir.mkdir(parents=True, exist_ok=True)
    bi5 = out_dir / f"{h:02d}.bi5"
    sent_404 = out_dir / f"{h:02d}.404"
    if bi5.exists() or sent_404.exists():
        return "skipped"
    url = URL_T.format(code=code, y=y, m0=m0, d=d, h=h)
    backoff = 0.5
    for i in range(retries):
        try:
            r = await client.get(url)
            if r.status_code == 200:
                bi5.write_bytes(r.content)
                return "fetched"
            if r.status_code == 404:
                sent_404.write_bytes(b"")
                return "fetched"
            if r.status_code in (429, 503):
                await asyncio.sleep(backoff + random.random()*0.3)
                backoff = min(backoff*2, 30)
                continue
            if i == retries-1:
                print(f"[ERR ] {sym} {y}-{m0+1:02d}-{d:02d} {h:02d}h HTTP {r.status_code}", flush=True)
                return "error"
        except Exception as e:
            if i == retries-1:
                print(f"[ERR ] {sym} {y}-{m0+1:02d}-{d:02d} {h:02d}h {e.__class__.__name__}", flush=True)
                return "error"
            await asyncio.sleep(backoff)
            backoff = min(backoff*2, 30)
    return "error"

async def worker(queue, client, stats):
    while True:
        try:
            task = queue.get_nowait()
        except asyncio.QueueEmpty:
            return
        kind = await fetch_one(client, task)
        stats[kind] = stats.get(kind, 0) + 1

async def reporter(stats, total, t0):
    while True:
        await asyncio.sleep(30)
        done = sum(stats.values())
        elapsed = max(1, time.time() - t0)
        rate = stats.get('fetched', 0) / elapsed
        print(f"[{time.strftime('%H:%M:%S')}] {done}/{total} ({100*done/total:.1f}%) | "
              f"fetched={stats.get('fetched',0)} skipped={stats.get('skipped',0)} "
              f"err={stats.get('error',0)} | rate={rate:.1f}/s", flush=True)
        if done >= total:
            return

async def main(chunk_path, conc):
    with open(chunk_path) as f:
        tasks = json.load(f)
    print(f"[CHUNK] {chunk_path}: {len(tasks)} tasks, conc={conc}", flush=True)
    queue = asyncio.Queue()
    for t in tasks:
        queue.put_nowait(t)
    limits = httpx.Limits(max_connections=conc, max_keepalive_connections=conc, keepalive_expiry=30)
    timeout = httpx.Timeout(connect=10, read=30, write=10, pool=10)
    headers = {"User-Agent": "Mozilla/5.0", "Accept-Encoding": "identity"}
    stats = {}
    t0 = time.time()
    async with httpx.AsyncClient(http2=True, limits=limits, timeout=timeout, headers=headers) as c:
        rep = asyncio.create_task(reporter(stats, len(tasks), t0))
        workers = [asyncio.create_task(worker(queue, c, stats)) for _ in range(conc)]
        await asyncio.gather(*workers, return_exceptions=True)
        rep.cancel()
    elapsed = time.time() - t0
    print(f"[DONE] fetched={stats.get('fetched',0)} skipped={stats.get('skipped',0)} "
          f"err={stats.get('error',0)} elapsed={elapsed:.0f}s "
          f"rate={stats.get('fetched',0)/max(1,elapsed):.1f}/s", flush=True)

if __name__ == "__main__":
    chunk = sys.argv[1] if len(sys.argv) > 1 else "chunks/chunk_000.json"
    conc = int(sys.argv[2]) if len(sys.argv) > 2 else 12
    asyncio.run(main(chunk, conc))
