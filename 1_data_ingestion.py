import os
import pymongo
import pandas as pd
import requests
from dotenv import load_dotenv
from datetime import datetime
import json
import random

# Load environment variables
load_dotenv()

# MongoDB Configuration
MONGO_PASSWORD = os.getenv('MONGO_PASSWORD')
MONGO_CONNECTION_STRING = os.getenv('MONGO_CONNECTION_STRING').replace('<db_password>', MONGO_PASSWORD)
MONGO_DB_NAME = "AkkuProject"

# SAMPLE SIZE CONFIGURATION
SAMPLE_SIZE = 2500  # Number of records to store from each dataset

# Connect to MongoDB
print("Connecting to MongoDB...")
client = pymongo.MongoClient(MONGO_CONNECTION_STRING)
db = client[MONGO_DB_NAME]

# Test connection
try:
    client.admin.command('ping')
    print("✓ Successfully connected to MongoDB!")
    print(f"✓ Using database: {MONGO_DB_NAME}")
except Exception as e:
    print(f"✗ Connection failed: {e}")
    exit()

# Collection names
EV_RAW_COLLECTION = "ev_data_raw"
NUTRITION_RAW_COLLECTION = "nutrition_data_raw"

print("\n" + "="*60)
print("DATASET 1: Electric Vehicle Population Data (JSON)")
print("="*60)

# Dataset 1: Electric Vehicle Population Data (Semi-structured JSON)
print("\nFetching Electric Vehicle data from API...")
EV_API_URL = "https://data.wa.gov/api/views/f6w7-q2d2/rows.json?accessType=DOWNLOAD"

try:
    response = requests.get(EV_API_URL, timeout=120)
    response.raise_for_status()
    ev_json_data = response.json()
    
    print(f"✓ Downloaded EV data successfully")
    print(f"  - Response size: {len(response.content) / (1024*1024):.2f} MB")
    
    # Store raw JSON in MongoDB
    collection_ev_raw = db[EV_RAW_COLLECTION]
    
    # Clear existing data
    collection_ev_raw.delete_many({})
    print("✓ Cleared existing EV data")
    
    # Extract column names from metadata
    columns_meta = ev_json_data.get("meta", {}).get("view", {}).get("columns", [])
    column_names = [col.get("name", f"col_{i}") for i, col in enumerate(columns_meta)]
    
    print(f"  - Columns found: {len(column_names)}")
    print(f"  - Total records available: {len(ev_json_data.get('data', [])):,}")
    
    # Store metadata separately
    metadata_doc = {
        "dataset_name": "Electric Vehicle Population Data",
        "source": "Washington State Department of Licensing",
        "format": "JSON (Semi-structured)",
        "sdg_goals": ["Goal 7: Clean Energy", "Goal 11: Sustainable Cities", "Goal 13: Climate Action"],
        "downloaded_at": datetime.now(),
        "api_url": EV_API_URL,
        "columns": column_names,
        "column_metadata": columns_meta,
        "total_available_records": len(ev_json_data.get("data", [])),
        "sampled_records": SAMPLE_SIZE,
        "sampling_method": "random",
        "is_metadata": True
    }
    collection_ev_raw.insert_one(metadata_doc)
    print("✓ Stored metadata document")
    
    # SAMPLE: Take random 2500 records instead of all
    ev_data_rows = ev_json_data.get("data", [])
    
    if len(ev_data_rows) > SAMPLE_SIZE:
        sampled_rows = random.sample(ev_data_rows, SAMPLE_SIZE)
        print(f"\n✓ Randomly sampled {SAMPLE_SIZE:,} records from {len(ev_data_rows):,} total records")
    else:
        sampled_rows = ev_data_rows
        print(f"\n✓ Using all {len(ev_data_rows):,} records (less than sample size)")
    
    # Convert to documents
    ev_documents = []
    for row in sampled_rows:
        doc = {
            "_imported_at": datetime.now(),
            "_dataset_name": "Electric Vehicle Population Data",
            "_source": "Washington State DOL"
        }
        
        # Map data to column names
        for idx, value in enumerate(row):
            if idx < len(column_names):
                doc[column_names[idx]] = value
        
        ev_documents.append(doc)
    
    # Insert all at once (faster for small datasets)
    print(f"\nInserting {len(ev_documents):,} EV records...")
    collection_ev_raw.insert_many(ev_documents)
    
    print(f"✓ Stored in MongoDB collection: '{EV_RAW_COLLECTION}'")
    print(f"  - Total records inserted: {len(ev_documents):,}")
    
except requests.exceptions.RequestException as e:
    print(f"✗ Error downloading EV data: {e}")
except Exception as e:
    print(f"✗ Error processing EV data: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "="*60)
print("DATASET 2: Nutrition, Physical Activity, and Obesity Data")
print("="*60)

# Dataset 2: Nutrition, Physical Activity, and Obesity
print("\nFetching Nutrition data from CDC API (limited to 2500 records)...")
# Directly request only 2500 records from API
NUTRITION_API_URL = f"https://chronicdata.cdc.gov/resource/hn4x-zwk7.json?$limit={SAMPLE_SIZE}"

try:
    response = requests.get(NUTRITION_API_URL, timeout=120)
    response.raise_for_status()
    nutrition_json_data = response.json()
    
    print(f"✓ Downloaded Nutrition data successfully")
    print(f"  - Response size: {len(response.content) / (1024*1024):.2f} MB")
    print(f"  - Total records fetched: {len(nutrition_json_data):,}")
    
    # Store raw JSON in MongoDB
    collection_nutrition_raw = db[NUTRITION_RAW_COLLECTION]
    
    # Clear existing data
    collection_nutrition_raw.delete_many({})
    print("✓ Cleared existing Nutrition data")
    
    # Add metadata to each document
    nutrition_documents = []
    for record in nutrition_json_data:
        record['_imported_at'] = datetime.now()
        record['_dataset_name'] = "Nutrition, Physical Activity, and Obesity - BRFSS"
        record['_source'] = "CDC - Behavioral Risk Factor Surveillance System"
        nutrition_documents.append(record)
    
    # Insert all at once
    print(f"\nInserting {len(nutrition_documents):,} Nutrition records...")
    collection_nutrition_raw.insert_many(nutrition_documents)
    
    # Store metadata document
    metadata_doc = {
        "dataset_name": "Nutrition, Physical Activity, and Obesity - BRFSS",
        "source": "CDC Behavioral Risk Factor Surveillance System",
        "format": "JSON API (Structured)",
        "sdg_goals": ["Goal 2: Zero Hunger", "Goal 3: Good Health and Well-being"],
        "downloaded_at": datetime.now(),
        "api_url": NUTRITION_API_URL,
        "sampled_records": len(nutrition_json_data),
        "sampling_method": "API limit parameter",
        "is_metadata": True
    }
    collection_nutrition_raw.insert_one(metadata_doc)
    
    print(f"✓ Stored in MongoDB collection: '{NUTRITION_RAW_COLLECTION}'")
    print(f"  - Total records inserted: {len(nutrition_documents):,}")
    
except requests.exceptions.RequestException as e:
    print(f"✗ Error downloading Nutrition data: {e}")
except Exception as e:
    print(f"✗ Error processing Nutrition data: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "="*60)
print("DATA INGESTION SUMMARY")
print("="*60)

# Display collections in database
collections = db.list_collection_names()
print(f"\n✓ Database: {MONGO_DB_NAME}")
print(f"✓ Collections created: {len(collections)}")
for col in collections:
    count = db[col].count_documents({})
    # Get sample document to show structure
    sample = db[col].find_one({"is_metadata": {"$ne": True}})
    print(f"\n  📁 Collection: {col}")
    print(f"     - Total documents: {count:,}")
    if sample:
        print(f"     - Sample fields: {list(sample.keys())[:10]}...")

print("\n" + "="*60)
print("✓ DATA INGESTION COMPLETE!")
print("="*60)
print(f"\n📊 Total records stored: {SAMPLE_SIZE * 2:,} records")
print(f"💾 Storage saved: Using only {SAMPLE_SIZE:,} records per dataset")
print(f"⏱️  Time saved: Much faster downloads and inserts!")
print("\nNext steps:")
print("1. Run data cleaning and transformation")
print("2. Perform exploratory data analysis")
print("3. Create visualizations")

# Close connection
client.close()
print("\n✓ MongoDB connection closed.")
