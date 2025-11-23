from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os, gzip, json

RAW_PATH = "/opt/airflow/data_raw"
STAGE_PATH = "/opt/airflow/data_stage"
CLEAN_PATH = "/opt/airflow/data_clean"
TRANSFORM_PATH = "/opt/airflow/data_transform"

MAX_NODES = 180000  # valor usado luego en filtro; puedes cambiarlo

def ensure_dirs():
    for p in (STAGE_PATH, CLEAN_PATH, TRANSFORM_PATH):
        os.makedirs(p, exist_ok=True)

# utilities
def convert_multiline_to_ndjson(input_path, output_path):
    print(f"[convert] {input_path} -> {output_path}")
    if input_path.endswith(".gz"):
        fin = gzip.open(input_path, "rt", encoding="utf-8", errors="ignore")
    else:
        fin = open(input_path, "r", encoding="utf-8", errors="ignore")
    fout = gzip.open(output_path, "wt", encoding="utf-8")
    buffer = ""
    for line in fin:
        line = line.strip()
        if not line:
            continue
        if line.startswith("{") and buffer:
            fout.write(buffer + "\n")
            buffer = line
        else:
            buffer += line
        if line.endswith("}"):
            fout.write(buffer + "\n")
            buffer = ""
    if buffer:
        fout.write(buffer + "\n")
    fin.close()
    fout.close()
    print(f"[convert] done: {output_path}")

# extract
def process_nodes():
    ensure_dirs()
    inp = f"{RAW_PATH}/nodes_raw.ndjson.gz"
    out = f"{STAGE_PATH}/nodes_raw_extracted.ndjson.gz"
    convert_multiline_to_ndjson(inp, out)

def process_edges():
    ensure_dirs()
    inp = f"{RAW_PATH}/edges_raw.ndjson.gz"
    out = f"{STAGE_PATH}/edges_raw_extracted.ndjson.gz"
    convert_multiline_to_ndjson(inp, out)

# clean
def clean_nodes():
    ensure_dirs()
    in_file = f"{STAGE_PATH}/nodes_raw_extracted.ndjson.gz"
    out_file = f"{CLEAN_PATH}/nodes_clean.ndjson.gz"
    cleaned = skipped = 0
    with gzip.open(in_file, "rt", encoding="utf-8") as fin, \
         gzip.open(out_file, "wt", encoding="utf-8") as fout:
        for line in fin:
            try:
                obj = json.loads(line)
                tx_id = str(obj.get("tx_id"))
                time_step = obj.get("time_step")
                if isinstance(time_step, str) and time_step.isnumeric():
                    time_step = int(time_step)
                elif not isinstance(time_step, int):
                    skipped += 1
                    continue
                features_raw = obj.get("features_raw")
                if not isinstance(features_raw, list) or len(features_raw) == 0:
                    skipped += 1
                    continue
                features = []
                for v in features_raw:
                    try:
                        features.append(float(v))
                    except:
                        features.append(None)
                fout.write(json.dumps({
                    "tx_id": tx_id,
                    "time_step": time_step,
                    "features": features
                }) + "\n")
                cleaned += 1
            except Exception:
                skipped += 1
                continue
    print(f"[clean_nodes] Nodes limpios: {cleaned} / descartados: {skipped}")

def clean_edges():
    ensure_dirs()
    in_file = f"{STAGE_PATH}/edges_raw_extracted.ndjson.gz"
    out_file = f"{CLEAN_PATH}/edges_clean.ndjson.gz"
    cleaned = skipped = 0
    with gzip.open(in_file, "rt", encoding="utf-8") as fin, \
         gzip.open(out_file, "wt", encoding="utf-8") as fout:
        for line in fin:
            try:
                obj = json.loads(line)
                tx1, tx2 = str(obj["tx1"]), str(obj["tx2"])
                if tx1 == tx2:
                    skipped += 1
                    continue
                fout.write(json.dumps({"tx1": tx1, "tx2": tx2}) + "\n")
                cleaned += 1
            except Exception:
                skipped += 1
                continue
    print(f"[clean_edges] Edges limpios: {cleaned} / descartados: {skipped}")

# transform 
def load_classes():
    import pandas as pd
    CLASSES_FILE = f"{RAW_PATH}/elliptic_txs_classes.csv"
    df = pd.read_csv(CLASSES_FILE, dtype=str)
    df.columns = [c.strip() for c in df.columns]
    if "tx_id" not in df.columns:
        df = df.rename(columns={df.columns[0]: "tx_id"})
    if "class" not in df.columns:
        df = df.rename(columns={df.columns[-1]: "class"})
    return dict(zip(df["tx_id"], df["class"]))

def transform_nodes():
    ensure_dirs()
    class_map = load_classes()
    in_file = f"{CLEAN_PATH}/nodes_clean.ndjson.gz"
    out_file = f"{TRANSFORM_PATH}/nodes_final.ndjson.gz"
    written = 0
    with gzip.open(in_file, "rt", encoding="utf-8") as fin, \
         gzip.open(out_file, "wt", encoding="utf-8") as fout:
        for line in fin:
            obj = json.loads(line)
            tx_id = obj["tx_id"]
            class_label = class_map.get(tx_id, "unknown")
            transformed = {
                "tx_id": tx_id,
                "time_step": obj["time_step"],
                "class": class_label,
                "features": obj["features"]
            }
            fout.write(json.dumps(transformed) + "\n")
            written += 1
    print(f"[transform_nodes] escritos: {written}")

def transform_edges():
    ensure_dirs()
    in_file = f"{CLEAN_PATH}/edges_clean.ndjson.gz"
    out_file = f"{TRANSFORM_PATH}/edges_final.ndjson.gz"
    written = 0
    with gzip.open(in_file, "rt", encoding="utf-8") as fin, \
         gzip.open(out_file, "wt", encoding="utf-8") as fout:
        for line in fin:
            obj = json.loads(line)
            fout.write(json.dumps({"src": obj["tx1"], "dst": obj["tx2"], "relation": "TRANSFER"}) + "\n")
            written += 1
    print(f"[transform_edges] escritos: {written}")

# filter
def filter_nodes_edges():
    ensure_dirs()
    nodes_in = f"{TRANSFORM_PATH}/nodes_final.ndjson.gz"
    edges_in = f"{TRANSFORM_PATH}/edges_final.ndjson.gz"
    out_nodes = f"{TRANSFORM_PATH}/nodes_final_filtered.ndjson.gz"
    out_edges = f"{TRANSFORM_PATH}/edges_final_filtered.ndjson.gz"
    valid_ids = set()
    count = 0
    with gzip.open(nodes_in, "rt", encoding="utf-8") as fin, \
         gzip.open(out_nodes, "wt", encoding="utf-8") as fout_nodes:
        for line in fin:
            if count >= MAX_NODES:
                break
            obj = json.loads(line)
            tx_id = obj["tx_id"]
            valid_ids.add(tx_id)
            fout_nodes.write(json.dumps(obj) + "\n")
            count += 1
    print(f"[filter] Nodos filtrados escritos: {count}")
    kept_edges = 0
    with gzip.open(edges_in, "rt", encoding="utf-8") as fin, \
         gzip.open(out_edges, "wt", encoding="utf-8") as fout_edges:
        for line in fin:
            obj = json.loads(line)
            if obj["src"] in valid_ids and obj["dst"] in valid_ids:
                fout_edges.write(json.dumps(obj) + "\n")
                kept_edges += 1
    print(f"[filter] Relaciones filtradas: {kept_edges}")

# DAG
with DAG(
    dag_id="extract_and_clean",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["elliptic","etl"]
) as dag:

    t_extract_nodes = PythonOperator(task_id="extract_nodes", python_callable=process_nodes)
    t_extract_edges = PythonOperator(task_id="extract_edges", python_callable=process_edges)

    t_clean_nodes = PythonOperator(task_id="clean_nodes", python_callable=clean_nodes)
    t_clean_edges = PythonOperator(task_id="clean_edges", python_callable=clean_edges)

    t_transform_nodes = PythonOperator(task_id="transform_nodes", python_callable=transform_nodes)
    t_transform_edges = PythonOperator(task_id="transform_edges", python_callable=transform_edges)

    t_filter = PythonOperator(task_id="filter_nodes_edges", python_callable=filter_nodes_edges)

    t_extract_nodes >> t_clean_nodes
    t_extract_nodes >> t_clean_edges
    t_extract_edges >> t_clean_nodes
    t_extract_edges >> t_clean_edges
    t_clean_nodes >> t_transform_nodes
    t_clean_nodes >> t_transform_edges
    t_clean_edges >> t_transform_nodes
    t_clean_edges >> t_transform_edges
    t_transform_nodes >> t_filter
    t_transform_edges >> t_filter
