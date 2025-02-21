import os
import json
import logging
import psycopg2
from psycopg2.extras import execute_values
import requests
from dotenv import load_dotenv
from db import get_db_connection

# Configure logging to stream to stdout.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

# Load environment variables from .env file
load_dotenv()

# Chroma (Vector DB) collection configuration
CHROMA_COLLECTION_NAME = os.getenv("CHROMA_COLLECTION_NAME", "my_collection")
BACKUP_TABLE = os.getenv("BACKUP_TABLE", "chroma_data")
NEW_COLLECTION_NAME = os.getenv("NEW_COLLECTION_NAME", "imported_collection")

# Remote Vector DB API configuration
CHROMADB_HOST = os.getenv("CHROMADB_HOST", "localhost")
CHROMADB_PORT = int(os.getenv("CHROMADB_PORT", 8000))
CHROMADB_USE_SSL = os.getenv("CHROMADB_USE_SSL", "False").lower() in ("true", "1", "yes")

def get_base_url():
    """
    Construct the base URL from the environment configuration.
    """
    scheme = "https" if CHROMADB_USE_SSL else "http"
    # Omit port if standard.
    if (CHROMADB_USE_SSL and CHROMADB_PORT == 443) or (not CHROMADB_USE_SSL and CHROMADB_PORT == 80):
        return f"{scheme}://{CHROMADB_HOST}"
    return f"{scheme}://{CHROMADB_HOST}:{CHROMADB_PORT}"

def get_backup_ids():
    """
    Fetch the set of IDs already backed up in the PostgreSQL table.
    If the table doesn't exist yet, return an empty set.
    """
    backup_ids = set()
    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(f"SELECT id FROM {BACKUP_TABLE};")
        rows = cur.fetchall()
        backup_ids = set(row[0] for row in rows)
        logging.info(f"Fetched {len(backup_ids)} backup IDs from table '{BACKUP_TABLE}'.")
    except Exception as e:
        if "does not exist" in str(e):
            logging.info(f"Backup table '{BACKUP_TABLE}' does not exist. Returning empty set.")
            backup_ids = set()
        else:
            logging.error(f"Error fetching backup IDs: {e}")
            backup_ids = set()
    finally:
        try:
            if cur:
                cur.close()
            if conn:
                conn.close()
        except Exception:
            pass
    return backup_ids

def export_collection_to_postgres():
    """
    Exports new records from the remote vector DB's collection to PostgreSQL.
    Only records with IDs not already in the backup table are inserted.
    """
    logging.info("Starting export process ...")
    base_url = get_base_url()
    get_url = f"{base_url}/api/v1/vector_db/collections/{CHROMA_COLLECTION_NAME}/embeddings"
    
    try:
        response = requests.get(get_url, headers={"accept": "application/json"})
        if response.status_code != 200:
            logging.error(f"Failed to retrieve embeddings from collection '{CHROMA_COLLECTION_NAME}'. Status: {response.status_code}")
            return
        # The API now returns a dict with keys: "ids", "embeddings", "metadatas", "documents", etc.
        data = response.json()
        logging.info(f"Retrieved data keys: {list(data.keys())} from collection '{CHROMA_COLLECTION_NAME}'.")
    except Exception as e:
        logging.error(f"Error retrieving data from remote API: {e}")
        return

    # Extract lists from the returned data
    ids_list = data.get("ids", [])
    embeddings_list = data.get("embeddings", [])
    metadatas_list = data.get("metadatas", [])
    documents_list = data.get("documents", [])
    
    logging.info(f"IDs count: {len(ids_list)}, embeddings count: {len(embeddings_list)}, metadatas count: {len(metadatas_list)}, documents count: {len(documents_list)}")
    
    # Get IDs already backed up.
    backup_ids = get_backup_ids()
    new_rows = []
    for i, id_val in enumerate(ids_list):
        if id_val not in backup_ids:
            embedding = embeddings_list[i] if i < len(embeddings_list) else None
            metadata = metadatas_list[i] if i < len(metadatas_list) else None
            document = documents_list[i] if i < len(documents_list) else None
            new_rows.append((id_val, json.dumps(embedding), json.dumps(metadata), document))
            logging.info(f"Adding new record: {id_val}")

    if not new_rows:
        logging.info("No new records to export.")
        return

    # Ensure backup table exists.
    conn = None
    cur = None
    try:
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
        logging.info(f"Ensured backup table '{BACKUP_TABLE}' exists.")
    except Exception as e:
        logging.error(f"Error ensuring backup table exists: {e}")
        return

    try:
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
        logging.info(f"Exported {len(new_rows)} new records to backup table '{BACKUP_TABLE}'.")
    except Exception as e:
        logging.error(f"Error upserting records: {e}")
    finally:
        try:
            if cur:
                cur.close()
            if conn:
                conn.close()
        except Exception:
            pass

def import_postgres_to_chroma():
    """
    Imports all backed-up records from PostgreSQL into a new collection
    in the remote vector DB.
    """
    logging.info("Starting import process ...")
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(f"SELECT id, embedding, metadata, document FROM {BACKUP_TABLE};")
        rows = cur.fetchall()
        logging.info(f"Fetched {len(rows)} records from backup table '{BACKUP_TABLE}'.")
    except Exception as e:
        logging.error(f"Error fetching backup data: {e}")
        return
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass

    if not rows:
        logging.info("No backup data found to import.")
        return

    # Prepare payload for adding embeddings.
    embeddings_payload = []
    for row in rows:
        id_val, emb_json, meta_json, doc = row
        try:
            if emb_json and isinstance(emb_json, str):
                embedding = json.loads(emb_json)
            else:
                embedding = emb_json
            if meta_json and isinstance(meta_json, str):
                metadata = json.loads(meta_json)
            else:
                metadata = meta_json
        except Exception as e:
            logging.error(f"Error parsing JSON for record {id_val}: {e}")
            continue
        embeddings_payload.append({
            "id": id_val,
            "embedding": embedding,
            "metadata": metadata,
            "document": doc
        })

    base_url = get_base_url()
    # Create new collection.
    create_url = f"{base_url}/api/v1/vector_db/collections"
    try:
        response = requests.post(create_url, json={"name": NEW_COLLECTION_NAME}, headers={"accept": "application/json"})
        if response.status_code == 200:
            logging.info(f"Created collection '{NEW_COLLECTION_NAME}'.")
        else:
            logging.warning(f"Collection '{NEW_COLLECTION_NAME}' creation returned status {response.status_code}. Proceeding with adding embeddings.")
    except Exception as e:
        logging.error(f"Error creating collection '{NEW_COLLECTION_NAME}': {e}")
        return

    # Add embeddings to the new collection.
    add_url = f"{base_url}/api/v1/vector_db/collections/{NEW_COLLECTION_NAME}/add_embeddings"
    try:
        response = requests.post(add_url, json=embeddings_payload, headers={"accept": "application/json", "Content-Type": "application/json"})
        if response.status_code == 200:
            logging.info(f"Imported {len(embeddings_payload)} records into new collection '{NEW_COLLECTION_NAME}'.")
        else:
            logging.error(f"Failed to import embeddings. Status: {response.status_code}. Response: {response.text}")
    except Exception as e:
        logging.error(f"Error importing records into collection '{NEW_COLLECTION_NAME}': {e}")

def check_collection_health():
    """
    Checks the health of the primary collection in the remote vector DB.
    If retrieval fails, trigger an import from backup.
    """
    logging.info("Performing health check on primary collection ...")
    base_url = get_base_url()
    health_url = f"{base_url}/api/v1/vector_db/collections/{CHROMA_COLLECTION_NAME}/embeddings"
    try:
        response = requests.get(health_url, headers={"accept": "application/json"})
        if response.status_code == 200:
            logging.info(f"Collection '{CHROMA_COLLECTION_NAME}' is healthy.")
        else:
            logging.error(f"Health check failed for collection '{CHROMA_COLLECTION_NAME}'. Status: {response.status_code}")
            logging.info("Triggering import from backup ...")
            import_postgres_to_chroma()
    except Exception as e:
        logging.error(f"Exception during health check for collection '{CHROMA_COLLECTION_NAME}': {e}")
        logging.info("Triggering import from backup ...")
        import_postgres_to_chroma()
