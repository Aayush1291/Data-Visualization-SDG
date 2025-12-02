import os
import pymongo
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from dotenv import load_dotenv
import warnings
warnings.filterwarnings('ignore')

# Set style for better-looking plots
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)

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
    print("✓ Connected to MongoDB!\n")
except Exception as e:
    print(f"✗ Connection failed: {e}")
    exit()

print("="*60)
print("EXPLORATORY DATA ANALYSIS")
print("="*60)

# Create output directory for plots
import os
if not os.path.exists('analysis_outputs'):
    os.makedirs('analysis_outputs')
print("✓ Created 'analysis_outputs' directory for charts\n")

# ============================================================
# DATASET 1: ELECTRIC VEHICLE ANALYSIS
# ============================================================

print("="*60)
print("DATASET 1: Electric Vehicle Population Analysis")
print("="*60)

# Load cleaned EV data
print("\n1. Loading EV data from MongoDB...")
ev_collection = db["ev_data_cleaned"]
ev_df = pd.DataFrame(list(ev_collection.find()))

print(f"✓ Loaded {len(ev_df):,} records")

# Basic Statistics
print("\n2. Dataset Overview:")
print(f"  - Total Records: {len(ev_df):,}")
print(f"  - Total Columns: {len(ev_df.columns)}")
print(f"  - Memory Usage: {ev_df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")

# Display column names and types
print("\n3. Column Information:")
col_info = pd.DataFrame({
    'Column': ev_df.columns,
    'Type': ev_df.dtypes.values,
    'Non-Null': ev_df.count().values,
    'Unique': [ev_df[col].nunique() for col in ev_df.columns]
})
print(col_info.to_string(index=False))

# Identify key columns for analysis
print("\n4. Identifying Key Columns for Analysis:")
# Find columns that might contain important EV information
key_columns = []
for col in ev_df.columns:
    if not col.startswith('_'):
        key_columns.append(col)

print(f"  ✓ Found {len(key_columns)} data columns (excluding internal fields)")

# Statistical Summary for numeric columns
numeric_cols = ev_df.select_dtypes(include=[np.number]).columns.tolist()
numeric_cols = [col for col in numeric_cols if not col.startswith('_')]

if len(numeric_cols) > 0:
    print(f"\n5. Statistical Summary (Numeric Columns):")
    print(ev_df[numeric_cols].describe().to_string())
else:
    print("\n5. No numeric columns found for statistical analysis")

# Categorical Analysis
categorical_cols = ev_df.select_dtypes(include=['object']).columns.tolist()
categorical_cols = [col for col in categorical_cols if not col.startswith('_') and col != '_id']

if len(categorical_cols) > 0:
    print(f"\n6. Categorical Column Analysis:")
    for col in categorical_cols[:5]:  # First 5 categorical columns
        unique_count = ev_df[col].nunique()
        print(f"\n  Column: '{col}'")
        print(f"    - Unique values: {unique_count}")
        if unique_count <= 20:
            value_counts = ev_df[col].value_counts().head(10)
            print(f"    - Top values:")
            for val, count in value_counts.items():
                print(f"      {val}: {count:,} ({count/len(ev_df)*100:.1f}%)")

# ============================================================
# DATASET 2: NUTRITION DATA ANALYSIS
# ============================================================

print("\n" + "="*60)
print("DATASET 2: Nutrition & Obesity Analysis")
print("="*60)

# Load cleaned Nutrition data
print("\n1. Loading Nutrition data from MongoDB...")
nutrition_collection = db["nutrition_data_cleaned"]
nutrition_df = pd.DataFrame(list(nutrition_collection.find()))

print(f"✓ Loaded {len(nutrition_df):,} records")

# Basic Statistics
print("\n2. Dataset Overview:")
print(f"  - Total Records: {len(nutrition_df):,}")
print(f"  - Total Columns: {len(nutrition_df.columns)}")
print(f"  - Memory Usage: {nutrition_df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")

# Key Metrics
print("\n3. Key Metrics Analysis:")

if 'locationdesc' in nutrition_df.columns:
    states_count = nutrition_df['locationdesc'].nunique()
    print(f"  - States/Locations covered: {states_count}")
    print(f"  - Top 5 states by record count:")
    top_states = nutrition_df['locationdesc'].value_counts().head(5)
    for state, count in top_states.items():
        print(f"    {state}: {count:,} records")

if 'yearstart' in nutrition_df.columns:
    years = nutrition_df['yearstart'].dropna().unique()
    print(f"\n  - Years covered: {sorted(years)}")
    print(f"  - Year range: {min(years):.0f} - {max(years):.0f}")

if 'class' in nutrition_df.columns:
    classes = nutrition_df['class'].nunique()
    print(f"\n  - Health categories: {classes}")
    print(f"  - Categories:")
    for cat, count in nutrition_df['class'].value_counts().items():
        print(f"    {cat}: {count:,} records")

if 'topic' in nutrition_df.columns:
    topics = nutrition_df['topic'].nunique()
    print(f"\n  - Topics covered: {topics}")

# Statistical Summary
numeric_cols = ['data_value', 'low_confidence_limit', 'high_confidence_limit', 
                'sample_size', 'confidence_interval_width']
numeric_cols_present = [col for col in numeric_cols if col in nutrition_df.columns]

if len(numeric_cols_present) > 0:
    print(f"\n4. Statistical Summary:")
    print(nutrition_df[numeric_cols_present].describe().to_string())

# ============================================================
# CORRELATION ANALYSIS
# ============================================================

print("\n" + "="*60)
print("CORRELATION ANALYSIS")
print("="*60)

# Nutrition data correlations
if len(numeric_cols_present) > 1:
    print("\nNutrition Dataset - Correlation Matrix:")
    correlation_matrix = nutrition_df[numeric_cols_present].corr()
    print(correlation_matrix.to_string())
    
    # Save correlation heatmap
    plt.figure(figsize=(10, 8))
    sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', center=0, 
                square=True, linewidths=1, fmt='.2f')
    plt.title('Nutrition Data - Correlation Matrix', fontsize=14, fontweight='bold')
    plt.tight_layout()
    plt.savefig('analysis_outputs/nutrition_correlation_heatmap.png', dpi=300, bbox_inches='tight')
    print("  ✓ Saved: analysis_outputs/nutrition_correlation_heatmap.png")
    plt.close()

# ============================================================
# TIME SERIES ANALYSIS
# ============================================================

print("\n" + "="*60)
print("TIME SERIES ANALYSIS")
print("="*60)

if 'yearstart' in nutrition_df.columns and 'data_value' in nutrition_df.columns:
    print("\nAnalyzing health trends over time...")
    
    # Calculate yearly averages
    yearly_avg = nutrition_df.groupby('yearstart')['data_value'].agg(['mean', 'count']).reset_index()
    
    print("\nYearly Statistics:")
    print(yearly_avg.to_string(index=False))
    
    # Plot time series
    plt.figure(figsize=(12, 6))
    plt.plot(yearly_avg['yearstart'], yearly_avg['mean'], marker='o', linewidth=2, markersize=8)
    plt.xlabel('Year', fontsize=12)
    plt.ylabel('Average Data Value', fontsize=12)
    plt.title('Health Indicators Trend Over Time', fontsize=14, fontweight='bold')
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig('analysis_outputs/health_trends_over_time.png', dpi=300, bbox_inches='tight')
    print("  ✓ Saved: analysis_outputs/health_trends_over_time.png")
    plt.close()

# ============================================================
# SUMMARY STATISTICS
# ============================================================

print("\n" + "="*60)
print("ANALYSIS SUMMARY")
print("="*60)

print("\n📊 Analysis Complete!")
print(f"\n✓ EV Dataset:")
print(f"  - Records analyzed: {len(ev_df):,}")
print(f"  - Numeric columns: {len([col for col in ev_df.select_dtypes(include=[np.number]).columns if not col.startswith('_')])}")
print(f"  - Categorical columns: {len([col for col in ev_df.select_dtypes(include=['object']).columns if not col.startswith('_')])}")

print(f"\n✓ Nutrition Dataset:")
print(f"  - Records analyzed: {len(nutrition_df):,}")
print(f"  - States covered: {nutrition_df['locationdesc'].nunique() if 'locationdesc' in nutrition_df.columns else 'N/A'}")
print(f"  - Years covered: {nutrition_df['yearstart'].nunique() if 'yearstart' in nutrition_df.columns else 'N/A'}")

print(f"\n✓ Outputs saved to: 'analysis_outputs/' directory")

print("\n" + "="*60)
print("Next step: Run '4_visualizations.py' for detailed charts!")
print("="*60)

# Close connection
client.close()
print("\n✓ MongoDB connection closed.")
