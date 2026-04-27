"""Read pre-split pending_tasks.json and chunk it for parallel matrix jobs."""
import json
from pathlib import Path

N_CHUNKS = 20  # matches workflow matrix max-parallel

def main():
    src = Path(__file__).parent / "pending_tasks.json"
    with open(src) as f:
        tasks = json.load(f)
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
