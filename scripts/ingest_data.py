import os
import json
import voyageai
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
MONGO_URI = os.getenv("MONGO_URI") or os.getenv("MONGODB_URI")
VOYAGE_API_KEY = os.getenv("VOYAGE_API_KEY")
DB_NAME = "epilog_db"
COLLECTION_NAME = "medical_guidelines"
DATA_FILE_PATH = os.path.join(os.path.dirname(__file__), "../data/guidelines.json")

if not VOYAGE_API_KEY:
    raise ValueError("VOYAGE_API_KEY is not set in environment variables.")
if not MONGO_URI:
    raise ValueError("MONGODB_URI is not set in environment variables.")

# Initialize Voyage AI Client
vo = voyageai.Client(api_key=VOYAGE_API_KEY)

# Initialize MongoDB Client
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

def load_data():
    try:
        with open(DATA_FILE_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"❌ Data file not found at: {DATA_FILE_PATH}")
        return []
    except json.JSONDecodeError:
        print(f"❌ Error decoding JSON from: {DATA_FILE_PATH}")
        return []

def ingest_data():
    print("🚀 Starting data ingestion...")
    
    data = load_data()
    if not data:
        print("⚠️ No data found to ingest.")
        return

    # 1. Clear existing data (Optional: Remove this if you want to append)
    # delete_result = collection.delete_many({})
    # print(f"🧹 Deleted {delete_result.deleted_count} existing documents.")
    
    # Check if we should append or replace. For now, let's keep replacement logic as default for consistency,
    # or just insert new ones. If you want to update, you might need a unique ID check.
    # Here, we will just INSERT (Append mode basically, unless we clear first).
    # Let's stick to "Delete All & Re-insert" for clean state management unless user asks otherwise.
    
    delete_result = collection.delete_many({})
    print(f"🧹 Deleted {delete_result.deleted_count} existing documents.")

    documents_to_insert = []
    
    # 2. Process and Embed Data
    texts_to_embed = [item["text"] for item in data]
    
    try:
        # Batch embedding
        print(f"🧠 Embedding {len(texts_to_embed)} documents with Voyage AI...")
        result = vo.embed(texts_to_embed, model="voyage-3-large", input_type="document")
        embeddings = result.embeddings
        
        for i, item in enumerate(data):
            doc = item.copy()
            doc["embedding"] = embeddings[i]
            documents_to_insert.append(doc)
            
    except Exception as e:
        print(f"❌ Error during embedding: {e}")
        return

    # 3. Insert into MongoDB
    if documents_to_insert:
        insert_result = collection.insert_many(documents_to_insert)
        print(f"✅ Successfully inserted {len(insert_result.inserted_ids)} documents.")
    else:
        print("⚠️ No documents to insert.")

    # 4. Output Search Index Definition
    print("\n" + "="*50)
    print("📋 MongoDB Atlas Search Index Definition (If not already created)")
    print("="*50)
    
    index_definition = {
        "mappings": {
            "dynamic": True,
            "fields": {
                "embedding": {
                    "dimensions": 1024,
                    "similarity": "cosine",
                    "type": "knnVector"
                }
            }
        }
    }
    
    print(json.dumps(index_definition, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    ingest_data()
