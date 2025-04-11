import json
from pathlib import Path
from collections import defaultdict
from typing import DefaultDict, Tuple
import seaborn as sns
import matplotlib.pyplot as plt

def tpch_duckdb_speedup():
    record: DefaultDict[Path, Tuple[int, float]] = defaultdict(list)
    for file in Path("report").rglob("tpch-sf*/duckdb/*.json"):
        threads = int(file.stem)
        latency = json.loads(file.read_text())['latency']
        record[file.parent].append((threads, latency))
    sns.set_theme(style="whitegrid")
    for base, data in sorted(record.items()):
        threads, latency = zip(*sorted(data))
        speedup = [latency[0] / t for t in latency]
        label = base.parent.as_posix().removeprefix("report/")
        sns.lineplot(x=threads, y=speedup, marker="o", label=label)
    plt.xlabel("Number of Threads")
    plt.ylabel("Speedup")
    plt.title("DuckDB GroupBy Speedup on TPC-H (SF1-SF100)")
    plt.legend(title="Dataset", loc="lower right")
    plt.savefig(Path("report", "tpch_duckdb_speedup.pdf"))

if __name__ == '__main__':
    tpch_duckdb_speedup()
