import os
import json
import psycopg2
from psycopg2.extras import execute_values
import chromadb
from dotenv import load_dotenv
from db import get_db_connection

# Load environment variables from .env file
load_dotenv()

# Configuration values for Chroma collection backup
CHROMA_COLLECTION_NAME = os.getenv("CHROMA_COLLECTION_NAME", "my_collection")
BACKUP_TABLE = os.getenv("BACKUP_TABLE", "chroma_data")
NEW_COLLECTION_NAME = os.getenv("NEW_COLLECTION_NAME", "imported_collection")

# Configuration values for connecting to the Chroma HTTP server
CHROMADB_HOST = os.getenv("CHROMADB_HOST", "localhost")
CHROMADB_PORT = int(os.getenv("CHROMADB_PORT", 8000))
CHROMADB_USE_SSL = os.getenv("CHROMADB_USE_SSL", "False").lower() in ("true", "1", "yes")

def get_backup_ids():
    """
    Fetch the set of IDs already backed up in the PostgreSQL table.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(f"SELECT id FROM {BACKUP_TABLE};")
    rows = cur.fetchall()
    backup_ids = set(row[0] for row in rows)
    cur.close()
    conn.close()
    return backup_ids

def export_collection_to_postgres():
    """
    Exports new records from the Chroma collection to PostgreSQL.
    Only records with IDs not already in the backup table are inserted.
    """
    print("Starting export ...")
    # Create an HTTP client instance for the remote Chroma server.
    client = chromadb.HttpClient(host=CHROMADB_HOST, port=CHROMADB_PORT, ssl=CHROMADB_USE_SSL)
    collection = client.get_collection(name=CHROMA_COLLECTION_NAME)

    # Retrieve all data from the collection.
    result = collection.get(include=["embeddings", "metadatas", "documents"])
    ids = result.get("ids", [])
    embeddings = result.get("embeddings", [])
    metadatas = result.get("metadatas", [])
    documents = result.get("documents", [])

    # Get IDs already backed up.
    backup_ids = get_backup_ids()
    new_rows = []
    for i, id_val in enumerate(ids):
        if id_val not in backup_ids:
            emb = embeddings[i] if i < len(embeddings) else None
            meta = metadatas[i] if i < len(metadatas) else None
            doc = documents[i] if i < len(documents) else None
            new_rows.append((id_val, json.dumps(emb), json.dumps(meta), doc))

    if not new_rows:
        print("No new records to export.")
        return

    # Ensure backup table exists in PostgreSQL.
    conn = get_db_connection()
    cur = conn.cursor()
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {BACKUP_TABLE} (
        id TEXT PRIMARY KEY,
        embedding JSONB,
        metadata JSONB,
        document TEXT
    );
    """
    cur.execute(create_table_query)
    conn.commit()

    # Upsert new rows.
    upsert_query = f"""
    INSERT INTO {BACKUP_TABLE} (id, embedding, metadata, document)
    VALUES %s
    ON CONFLICT (id)
    DO UPDATE SET
      embedding = EXCLUDED.embedding,
      metadata = EXCLUDED.metadata,
      document = EXCLUDED.document;
    """
    execute_values(cur, upsert_query, new_rows)
    conn.commit()
    cur.close()
    conn.close()
    print(f"Exported {len(new_rows)} new records from collection '{CHROMA_COLLECTION_NAME}' to PostgreSQL.")

def import_postgres_to_chroma():
    """
    Imports all backed-up records from PostgreSQL to re-create a Chroma collection.
    """
    print("Starting import ...")
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(f"SELECT id, embedding, metadata, document FROM {BACKUP_TABLE};")
    rows = cur.fetchall()
    cur.close()
    conn.close()

    if not rows:
        print("No backup data found.")
        return

    # Prepare lists for the collection.
    ids, embeddings, metadatas, documents = [], [], [], []
    for row in rows:
        id_val, emb_json, meta_json, doc = row
        ids.append(id_val)
        embeddings.append(json.loads(emb_json) if emb_json else None)
        metadatas.append(json.loads(meta_json) if meta_json else None)
        documents.append(doc)

    # Create an HTTP client instance.
    client = chromadb.HttpClient(host=CHROMADB_HOST, port=CHROMADB_PORT, ssl=CHROMADB_USE_SSL)
    # Optionally delete any previous instance of the backup collection.
    try:
        client.delete_collection(name=NEW_COLLECTION_NAME)
        print(f"Deleted existing collection '{NEW_COLLECTION_NAME}'.")
    except Exception:
        # If deletion fails (e.g. collection does not exist), ignore.
        pass

    collection = client.create_collection(name=NEW_COLLECTION_NAME)
    collection.add(ids=ids, embeddings=embeddings, metadatas=metadatas, documents=documents)
    print(f"Imported {len(ids)} records into new Chroma collection '{NEW_COLLECTION_NAME}'.")

def check_collection_health():
    """
    Checks the health of the primary Chroma collection.
    If retrieval fails, trigger an import from backup.
    """
    print("Performing health check ...")
    client = chromadb.HttpClient(host=CHROMADB_HOST, port=CHROMADB_PORT, ssl=CHROMADB_USE_SSL)
    try:
        _ = client.get_collection(name=CHROMA_COLLECTION_NAME)
        print(f"Collection '{CHROMA_COLLECTION_NAME}' is healthy.")
    except Exception as e:
        print(f"Failed to retrieve collection '{CHROMA_COLLECTION_NAME}': {e}")
        print("Triggering import from backup ...")
        import_postgres_to_chroma()
