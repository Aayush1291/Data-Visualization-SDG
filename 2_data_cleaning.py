import os
import pymongo
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

# MongoDB Configuration
MONGO_PASSWORD = os.getenv('MONGO_PASSWORD')
MONGO_CONNECTION_STRING = os.getenv('MONGO_CONNECTION_STRING').replace('<db_password>', MONGO_PASSWORD)
MONGO_DB_NAME = "AkkuProject"

# Connect to MongoDB
print("Connecting to MongoDB...")
client = pymongo.MongoClient(MONGO_CONNECTION_STRING)
db = client[MONGO_DB_NAME]

try:
    client.admin.command('ping')
    print("✓ Connected to MongoDB!")
    print(f"✓ Database: {MONGO_DB_NAME}\n")
except Exception as e:
    print(f"✗ Connection failed: {e}")
    exit()

print("="*60)
print("DATA CLEANING & TRANSFORMATION")
print("="*60)

# ============================================================
# DATASET 1: ELECTRIC VEHICLE DATA CLEANING
# ============================================================

print("\n" + "="*60)
print("DATASET 1: Electric Vehicle Population Data")
print("="*60)

# Load EV data from MongoDB
print("\n1. Loading EV data from MongoDB...")
ev_collection = db["ev_data_raw"]
ev_cursor = ev_collection.find({"is_metadata": {"$ne": True}})
ev_df = pd.DataFrame(list(ev_cursor))

print(f"✓ Loaded {len(ev_df):,} records")
print(f"  - Columns: {len(ev_df.columns)}")

# Display initial data info
print("\n2. Initial Data Assessment:")
print(f"  - Shape: {ev_df.shape}")
print(f"  - Memory usage: {ev_df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")

# Check for missing values
print("\n3. Missing Values Analysis:")
missing_counts = ev_df.isnull().sum()
missing_percent = (missing_counts / len(ev_df) * 100).round(2)
missing_df = pd.DataFrame({
    'Column': missing_counts.index,
    'Missing_Count': missing_counts.values,
    'Missing_Percent': missing_percent.values
})
missing_df = missing_df[missing_df['Missing_Count'] > 0].sort_values('Missing_Count', ascending=False)
if len(missing_df) > 0:
    print(f"  - Columns with missing values: {len(missing_df)}")
    print(missing_df.head(10).to_string(index=False))
else:
    print("  - No missing values found!")

# Data Cleaning Steps
print("\n4. Data Cleaning Steps:")

# Remove MongoDB internal fields
columns_to_drop = ['_id', '_imported_at', '_dataset_name', '_source']
ev_df_clean = ev_df.drop(columns=[col for col in columns_to_drop if col in ev_df.columns])
print(f"  ✓ Removed internal MongoDB fields")

# Handle specific columns based on typical EV dataset structure
# Get actual column names
actual_columns = list(ev_df_clean.columns)
print(f"  ✓ Working with {len(actual_columns)} columns")

# Convert numeric columns
print("\n5. Data Type Conversions:")
numeric_conversions = 0
for col in ev_df_clean.columns:
    # Try to convert to numeric if possible
    try:
        # Check if column might be numeric
        sample_val = ev_df_clean[col].dropna().iloc[0] if len(ev_df_clean[col].dropna()) > 0 else None
        if sample_val and isinstance(sample_val, (int, float, str)):
            if str(sample_val).replace('.', '').replace('-', '').isdigit():
                ev_df_clean[col] = pd.to_numeric(ev_df_clean[col], errors='coerce')
                numeric_conversions += 1
    except:
        pass

print(f"  ✓ Converted {numeric_conversions} columns to numeric types")

# Fill missing values with appropriate defaults
print("\n6. Handling Missing Values:")
for col in ev_df_clean.columns:
    missing_count = ev_df_clean[col].isnull().sum()
    if missing_count > 0:
        if ev_df_clean[col].dtype in ['float64', 'int64']:
            # Fill numeric with median
            ev_df_clean[col].fillna(ev_df_clean[col].median(), inplace=True)
        else:
            # Fill categorical with 'Unknown'
            ev_df_clean[col].fillna('Unknown', inplace=True)

print(f"  ✓ Filled all missing values")
print(f"  ✓ Remaining missing values: {ev_df_clean.isnull().sum().sum()}")

# Add derived columns
print("\n7. Feature Engineering:")
ev_df_clean['_processed_at'] = datetime.now()
ev_df_clean['_record_id'] = range(1, len(ev_df_clean) + 1)
print(f"  ✓ Added processed timestamp and record IDs")

# Store cleaned data
print("\n8. Storing Cleaned Data to MongoDB:")
ev_clean_collection = db["ev_data_cleaned"]
ev_clean_collection.delete_many({})

# Convert DataFrame to dict for MongoDB
ev_clean_records = ev_df_clean.to_dict('records')
ev_clean_collection.insert_many(ev_clean_records)

print(f"  ✓ Stored {len(ev_clean_records):,} cleaned records")
print(f"  ✓ Collection: 'ev_data_cleaned'")

# ============================================================
# DATASET 2: NUTRITION DATA CLEANING
# ============================================================

print("\n" + "="*60)
print("DATASET 2: Nutrition, Physical Activity, and Obesity Data")
print("="*60)

# Load Nutrition data from MongoDB
print("\n1. Loading Nutrition data from MongoDB...")
nutrition_collection = db["nutrition_data_raw"]
nutrition_cursor = nutrition_collection.find({"is_metadata": {"$ne": True}})
nutrition_df = pd.DataFrame(list(nutrition_cursor))

print(f"✓ Loaded {len(nutrition_df):,} records")
print(f"  - Columns: {len(nutrition_df.columns)}")

# Display initial data info
print("\n2. Initial Data Assessment:")
print(f"  - Shape: {nutrition_df.shape}")
print(f"  - Memory usage: {nutrition_df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")

# Check for missing values
print("\n3. Missing Values Analysis:")
missing_counts = nutrition_df.isnull().sum()
missing_percent = (missing_counts / len(nutrition_df) * 100).round(2)
missing_df = pd.DataFrame({
    'Column': missing_counts.index,
    'Missing_Count': missing_counts.values,
    'Missing_Percent': missing_percent.values
})
missing_df = missing_df[missing_df['Missing_Count'] > 0].sort_values('Missing_Count', ascending=False)
if len(missing_df) > 0:
    print(f"  - Columns with missing values: {len(missing_df)}")
    print(missing_df.head(10).to_string(index=False))
else:
    print("  - No missing values found!")

# Data Cleaning Steps
print("\n4. Data Cleaning Steps:")

# Remove MongoDB internal fields
columns_to_drop = ['_id', '_imported_at', '_dataset_name', '_source']
nutrition_df_clean = nutrition_df.drop(columns=[col for col in columns_to_drop if col in nutrition_df.columns])
print(f"  ✓ Removed internal MongoDB fields")

# Convert numeric columns
print("\n5. Data Type Conversions:")

# Known numeric columns in nutrition dataset
numeric_cols = ['data_value', 'data_value_alt', 'low_confidence_limit', 
                'high_confidence_limit', 'sample_size']

for col in numeric_cols:
    if col in nutrition_df_clean.columns:
        nutrition_df_clean[col] = pd.to_numeric(nutrition_df_clean[col], errors='coerce')
        print(f"  ✓ Converted '{col}' to numeric")

# Convert year columns
year_cols = ['yearstart', 'yearend']
for col in year_cols:
    if col in nutrition_df_clean.columns:
        nutrition_df_clean[col] = pd.to_numeric(nutrition_df_clean[col], errors='coerce')
        print(f"  ✓ Converted '{col}' to numeric")

# Handle missing values
print("\n6. Handling Missing Values:")
initial_missing = nutrition_df_clean.isnull().sum().sum()
print(f"  - Initial missing values: {initial_missing:,}")

for col in nutrition_df_clean.columns:
    missing_count = nutrition_df_clean[col].isnull().sum()
    if missing_count > 0:
        if nutrition_df_clean[col].dtype in ['float64', 'int64']:
            # Fill numeric with median
            nutrition_df_clean[col].fillna(nutrition_df_clean[col].median(), inplace=True)
        else:
            # Fill categorical with 'Unknown'
            nutrition_df_clean[col].fillna('Unknown', inplace=True)

final_missing = nutrition_df_clean.isnull().sum().sum()
print(f"  ✓ Filled missing values")
print(f"  - Remaining missing values: {final_missing}")

# Feature Engineering
print("\n7. Feature Engineering:")

# Add year range column
if 'yearstart' in nutrition_df_clean.columns and 'yearend' in nutrition_df_clean.columns:
    nutrition_df_clean['year_range'] = nutrition_df_clean['yearstart'].astype(str) + '-' + nutrition_df_clean['yearend'].astype(str)
    print(f"  ✓ Created 'year_range' column")

# Add confidence interval width
if 'low_confidence_limit' in nutrition_df_clean.columns and 'high_confidence_limit' in nutrition_df_clean.columns:
    nutrition_df_clean['confidence_interval_width'] = nutrition_df_clean['high_confidence_limit'] - nutrition_df_clean['low_confidence_limit']
    print(f"  ✓ Created 'confidence_interval_width' column")

# Add processed timestamp
nutrition_df_clean['_processed_at'] = datetime.now()
nutrition_df_clean['_record_id'] = range(1, len(nutrition_df_clean) + 1)
print(f"  ✓ Added processed timestamp and record IDs")

# Store cleaned data
print("\n8. Storing Cleaned Data to MongoDB:")
nutrition_clean_collection = db["nutrition_data_cleaned"]
nutrition_clean_collection.delete_many({})

# Convert DataFrame to dict for MongoDB
nutrition_clean_records = nutrition_df_clean.to_dict('records')
nutrition_clean_collection.insert_many(nutrition_clean_records)

print(f"  ✓ Stored {len(nutrition_clean_records):,} cleaned records")
print(f"  ✓ Collection: 'nutrition_data_cleaned'")

# ============================================================
# SUMMARY
# ============================================================

print("\n" + "="*60)
print("CLEANING SUMMARY")
print("="*60)

collections = db.list_collection_names()
print(f"\n✓ Database: {MONGO_DB_NAME}")
print(f"✓ Total Collections: {len(collections)}\n")

for col in collections:
    count = db[col].count_documents({})
    print(f"  📁 {col}")
    print(f"     - Documents: {count:,}")

print("\n" + "="*60)
print("✓ DATA CLEANING COMPLETE!")
print("="*60)

print("\n📊 Cleaned Datasets Summary:")
print(f"  - EV Data: {len(ev_df_clean):,} records, {len(ev_df_clean.columns)} columns")
print(f"  - Nutrition Data: {len(nutrition_df_clean):,} records, {len(nutrition_df_clean.columns)} columns")

print("\n💾 Exporting to CSV for backup...")
ev_df_clean.to_csv('ev_data_cleaned.csv', index=False)
nutrition_df_clean.to_csv('nutrition_data_cleaned.csv', index=False)
print("  ✓ Exported: ev_data_cleaned.csv")
print("  ✓ Exported: nutrition_data_cleaned.csv")

print("\nNext step: Run '3_data_analysis.py' for exploratory analysis!")

# Close connection
client.close()
print("\n✓ MongoDB connection closed.")
