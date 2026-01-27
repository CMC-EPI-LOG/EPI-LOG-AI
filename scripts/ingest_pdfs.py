import os
import shutil
import voyageai
from pymongo import MongoClient
from dotenv import load_dotenv
from PyPDF2 import PdfReader
from datetime import datetime
import time

# Load environment variables
load_dotenv()

# Configuration
MONGO_URI = os.getenv("MONGO_URI") or os.getenv("MONGODB_URI")
VOYAGE_API_KEY = os.getenv("VOYAGE_API_KEY")
DB_NAME = "epilog_db"
COLLECTION_NAME = "medical_guidelines"
UPLOAD_DIR = os.path.join(os.path.dirname(__file__), "../upload")

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

def embed_with_retry(texts, model, input_type, max_retries=10):
    """
    Retry wrapper for embedding to handle Rate Limits (TPM/RPM).
    """
    delay = 60 # Initial delay (increased for safety)
    for attempt in range(max_retries):
        try:
            return vo.embed(texts, model=model, input_type=input_type)
        except Exception as e:
            if "rate limit" in str(e).lower() or "429" in str(e) or "403" in str(e): # 403 often returns for limit on free tier
                print(f"⏳ Rate limit hit. Waiting {delay}s before retry ({attempt + 1}/{max_retries})...")
                time.sleep(delay)
                delay *= 2 # Exponential backoff
            else:
                raise e
    raise Exception("Max retries exceeded for embedding.")

def process_pdf(file_path):
    print(f"📄 Processing {os.path.basename(file_path)}...")
    try:
        reader = PdfReader(file_path)
        documents_to_insert = []
        
        # Process page by page instead of batching entire file to respect TPM
        # 10K TPM is very low (approx 10-20 pages depending on density).
        # We will embed 2 pages at a time to be extremely safe.
        
        BATCH_SIZE = 2
        current_batch_texts = []
        current_batch_indices = []
        
        for i, page in enumerate(reader.pages):
            text = page.extract_text()
            if text and len(text.strip()) > 50:
                current_batch_texts.append(text)
                current_batch_indices.append(i + 1)
                
                if len(current_batch_texts) >= BATCH_SIZE:
                    # Embed batch
                    print(f"🧠 Embedding pages {current_batch_indices[0]}-{current_batch_indices[-1]}...")
                    result = embed_with_retry(current_batch_texts, model="voyage-3-large", input_type="document")
                    embeddings = result.embeddings
                    
                    for k, emb in enumerate(embeddings):
                        documents_to_insert.append({
                            "text": current_batch_texts[k],
                            "category": "pdf_batch_upload",
                            "source": os.path.basename(file_path),
                            "page": current_batch_indices[k],
                            "risk_level": "unknown", 
                            "created_at": datetime.now(),
                            "embedding": emb
                        })
                    
                    # Reset batch
                    current_batch_texts = []
                    current_batch_indices = []
                    # Sleep to be safe (TPM cooling)
                    print("⏳ Waiting 30s to clear TPM limit...")
                    time.sleep(30)
        
        # Process remaining
        if current_batch_texts:
            print(f"🧠 Embedding remaining {len(current_batch_texts)} pages...")
            result = embed_with_retry(current_batch_texts, model="voyage-3-large", input_type="document")
            embeddings = result.embeddings
            
            for k, emb in enumerate(embeddings):
                documents_to_insert.append({
                    "text": current_batch_texts[k],
                    "category": "pdf_batch_upload",
                    "source": os.path.basename(file_path),
                    "page": current_batch_indices[k],
                    "risk_level": "unknown", 
                    "created_at": datetime.now(),
                    "embedding": emb
                })
            print("⏳ Waiting 30s to clear TPM limit...")
            time.sleep(30)

        if not documents_to_insert:
            print(f"⚠️ No documents successfully processed for {os.path.basename(file_path)}.")
            return False
            
        # Insert into DB
        collection.insert_many(documents_to_insert)
        print(f"✅ Indexed {len(documents_to_insert)} pages from {os.path.basename(file_path)}.")
        return True
            
    except Exception as e:
        print(f"❌ Error processing {os.path.basename(file_path)}: {e}")
        return False

def ingest_pdfs_from_folder():
    print(f"🚀 Starting bulk PDF ingestion from: {UPLOAD_DIR}")
    
    if not os.path.exists(UPLOAD_DIR):
        print(f"📁 Directory {UPLOAD_DIR} does not exist. Creating it...")
        os.makedirs(UPLOAD_DIR)
        print("Please put your PDF files in the 'upload' folder and run this script again.")
        return

    files = [f for f in os.listdir(UPLOAD_DIR) if f.lower().endswith('.pdf')]
    
    if not files:
        print("📭 No PDF files found in 'upload' directory.")
        return

    success_count = 0
    
    for filename in files:
        file_path = os.path.join(UPLOAD_DIR, filename)
        if process_pdf(file_path):
            try:
                os.remove(file_path)
                print(f"🗑️ Deleted processed file: {filename}")
                success_count += 1
                # Respect Rate Limit (3 RPM = 1 request every 20s. Safe bet: 25s)
                print("⏳ Waiting 25 seconds to respect Voyage AI Rate Limit (3 RPM)...")
                time.sleep(25)
            except OSError as e:
                print(f"⚠️ Failed to delete {filename}: {e}")
        else:
            print(f"⏭️ Skipping file deletion for {filename} due to errors.")
            
    print("\n" + "="*50)
    print(f"🎉 Completed! Ingested and removed {success_count}/{len(files)} files.")
    print("="*50)

if __name__ == "__main__":
    ingest_pdfs_from_folder()
