import gzip, json
from pathlib import Path
import pandas as pd

DATA_DIR = Path("C:/Users/LENOVO/Desktop/bigdata - proyecto/dataset")
OUT = Path("./json_raw")
OUT.mkdir(exist_ok=True)

feat = DATA_DIR / "elliptic_txs_features.csv"
with gzip.open(OUT / "./nodes_raw.ndjson.gz", "wt", encoding="utf-8") as fout:
    for chunk in pd.read_csv(feat, header=None, chunksize=10000, dtype=str):
        for _, r in chunk.iterrows():
            obj = {
                "tx_id": str(r[0]),
                "time_step": int(r[1]) if str(r[1]).isdigit() else r[1],
                "features_raw": [v for v in r.iloc[2:].tolist()]  # keep raw strings
            }
            fout.write(json.dumps(obj) + "\n")

edges = DATA_DIR / "./elliptic_txs_edgelist.csv"
with gzip.open(OUT / "./edges_raw.ndjson.gz", "wt", encoding="utf-8") as fout:
    for chunk in pd.read_csv(edges, header=None, names=["tx1","tx2"], chunksize=100000, dtype=str):
        for _, r in chunk.iterrows():
            fout.write(json.dumps({"tx1": str(r["tx1"]), "tx2": str(r["tx2"])}) + "\n")

print("Raw NDJSON creados en:", OUT)
