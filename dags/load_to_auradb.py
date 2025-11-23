# dags/load_to_nosql.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
from neo4j import GraphDatabase, exceptions as neo4j_exceptions
import json, os, gzip, time

TRANSFORM_PATH = "/opt/airflow/data_transform"
BATCH_SIZE = int(os.getenv("NEO4J_BATCH_SIZE", "500"))
MAX_RETRIES_PER_BATCH = int(os.getenv("NEO4J_MAX_RETRIES", "5"))
RETRY_BACKOFF_SEC = int(os.getenv("NEO4J_RETRY_BACKOFF", "3"))
CLEAN_BEFORE = os.getenv("NEO4J_CLEAN_BEFORE_LOAD", "false").lower() == "true"

def open_maybe_gz(path, mode="rt"):
    if path.endswith(".gz"):
        return gzip.open(path, mode, encoding="utf-8")
    return open(path, mode, encoding="utf-8")

def get_neo4j_driver():
    conn = BaseHook.get_connection("neo4j_conn")
    uri = f"neo4j+s://{conn.host}:{conn.port}"
    auth = (conn.login, conn.password)
    return GraphDatabase.driver(uri, auth=auth)

def stream_batches(file_path, batch_size=BATCH_SIZE):
    with open_maybe_gz(file_path, "rt") as f:
        batch = []
        for line in f:
            if not line.strip():
                continue
            obj = json.loads(line)
            batch.append(obj)
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

CREATE_NODE_QUERY = """
UNWIND $rows AS row
MERGE (t:Transaction {tx_id: row.tx_id})
SET t.time_step = row.time_step,
    t.class = row.class,
    t.features = row.features
"""

CREATE_EDGE_QUERY = """
UNWIND $rows AS row
MATCH (a:Transaction {tx_id: row.src})
MATCH (b:Transaction {tx_id: row.dst})
MERGE (a)-[:TRANSFER]->(b)
"""

def ensure_index_tx_id():
    driver = get_neo4j_driver()
    try:
        with driver.session() as session:
            session.run("CREATE INDEX IF NOT EXISTS FOR (t:Transaction) ON (t.tx_id)")
            print("[neo4j] índice verificado/creado")
    finally:
        driver.close()

def clean_db_transactions():
    if not CLEAN_BEFORE:
        print("[neo4j] CLEAN_BEFORE not enabled; saltando borrado")
        return
    driver = get_neo4j_driver()
    try:
        with driver.session() as session:
            session.run("MATCH (t:Transaction) DETACH DELETE t")
            print("[neo4j] Todos los nodos Transaction eliminados")
    finally:
        driver.close()

def load_with_batches(file_path, query, entity_name):
    driver = get_neo4j_driver()
    total = 0
    try:
        for i, batch in enumerate(stream_batches(file_path, BATCH_SIZE), start=1):
            attempt = 0
            while attempt < MAX_RETRIES_PER_BATCH:
                try:
                    with driver.session() as session:
                        session.run(query, rows=batch)
                    total += len(batch)
                    print(f"[neo4j] Batch {i}: {len(batch)} {entity_name}. Total={total}")
                    break
                except (neo4j_exceptions.ServiceUnavailable,
                        neo4j_exceptions.TransientError,
                        neo4j_exceptions.SessionExpired) as e:
                    attempt += 1
                    wait = RETRY_BACKOFF_SEC * (2 ** (attempt - 1))
                    print(f"[WARN] Batch {i} fallo intento {attempt}: {e}. Reintentando en {wait}s...")
                    time.sleep(wait)
            else:
                raise RuntimeError(f"Batch {i} falló después de {MAX_RETRIES_PER_BATCH} intentos.")
    finally:
        driver.close()
    print(f"[neo4j] {entity_name} insertados: {total}")

def load_nodes():
    file = f"{TRANSFORM_PATH}/nodes_final_filtered.ndjson.gz"
    if not os.path.exists(file):
        raise FileNotFoundError(file)
    load_with_batches(file, CREATE_NODE_QUERY, "nodos")

def load_edges():
    file = f"{TRANSFORM_PATH}/edges_final_filtered.ndjson.gz"
    if not os.path.exists(file):
        raise FileNotFoundError(file)
    load_with_batches(file, CREATE_EDGE_QUERY, "edges")

with DAG(
    dag_id="load_to_auradb",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["neo4j","load"]
) as dag:

    t_ensure_index = PythonOperator(task_id="ensure_index", python_callable=ensure_index_tx_id)
    t_clean_db = PythonOperator(task_id="clean_db_transactions", python_callable=clean_db_transactions)
    t_load_nodes = PythonOperator(task_id="load_nodes", python_callable=load_nodes)
    t_load_edges = PythonOperator(task_id="load_edges", python_callable=load_edges)

    t_ensure_index >> t_clean_db >> t_load_nodes >> t_load_edges
