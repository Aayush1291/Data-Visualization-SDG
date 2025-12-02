import os
import pymongo
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from dotenv import load_dotenv
import warnings
warnings.filterwarnings('ignore')

# Set style
sns.set_style("whitegrid")
sns.set_palette("husl")

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
print("DATA VISUALIZATIONS")
print("="*60)

# Create output directory
if not os.path.exists('visualizations'):
    os.makedirs('visualizations')
print("✓ Created 'visualizations' directory\n")

# ============================================================
# LOAD DATASETS
# ============================================================

print("Loading datasets...")
ev_df = pd.DataFrame(list(db["ev_data_cleaned"].find()))
nutrition_df = pd.DataFrame(list(db["nutrition_data_cleaned"].find()))
print(f"✓ EV Data: {len(ev_df):,} records")
print(f"✓ Nutrition Data: {len(nutrition_df):,} records\n")

# ============================================================
# VISUALIZATION 1: EV DATA ANALYSIS
# ============================================================

print("="*60)
print("Creating EV Visualizations...")
print("="*60)

# Find categorical columns for EV data
ev_categorical = [col for col in ev_df.select_dtypes(include=['object']).columns 
                  if not col.startswith('_') and col != '_id']

if len(ev_categorical) > 0:
    # Take first categorical column with reasonable unique values
    for col in ev_categorical:
        if 5 <= ev_df[col].nunique() <= 50:
            print(f"\n1. Distribution by {col}:")
            
            plt.figure(figsize=(14, 8))
            value_counts = ev_df[col].value_counts().head(15)
            
            ax = value_counts.plot(kind='barh', color='steelblue')
            plt.xlabel('Count', fontsize=12)
            plt.ylabel(col, fontsize=12)
            plt.title(f'Electric Vehicle Distribution by {col}', 
                     fontsize=14, fontweight='bold', pad=20)
            
            # Add value labels
            for i, v in enumerate(value_counts.values):
                ax.text(v + max(value_counts.values)*0.01, i, f'{v:,}', 
                       va='center', fontsize=10)
            
            plt.tight_layout()
            plt.savefig(f'visualizations/ev_distribution_{col.replace(" ", "_")}.png', 
                       dpi=300, bbox_inches='tight')
            print(f"  ✓ Saved: visualizations/ev_distribution_{col.replace(' ', '_')}.png")
            plt.close()
            break

# ============================================================
# VISUALIZATION 2: NUTRITION DATA - GEOGRAPHIC ANALYSIS
# ============================================================

print("\n" + "="*60)
print("Creating Nutrition Visualizations...")
print("="*60)

if 'locationdesc' in nutrition_df.columns and 'data_value' in nutrition_df.columns:
    print("\n2. Health Indicators by State (Top 20):")
    
    state_avg = nutrition_df.groupby('locationdesc')['data_value'].mean().sort_values(ascending=False).head(20)
    
    plt.figure(figsize=(14, 8))
    ax = state_avg.plot(kind='barh', color='coral')
    plt.xlabel('Average Data Value', fontsize=12)
    plt.ylabel('State', fontsize=12)
    plt.title('Average Health Indicators by State (Top 20)', 
             fontsize=14, fontweight='bold', pad=20)
    
    # Add value labels
    for i, v in enumerate(state_avg.values):
        ax.text(v + max(state_avg.values)*0.01, i, f'{v:.1f}', 
               va='center', fontsize=9)
    
    plt.tight_layout()
    plt.savefig('visualizations/health_by_state.png', dpi=300, bbox_inches='tight')
    print("  ✓ Saved: visualizations/health_by_state.png")
    plt.close()

# ============================================================
# VISUALIZATION 3: CLASS/TOPIC ANALYSIS
# ============================================================

if 'class' in nutrition_df.columns:
    print("\n3. Health Categories Distribution:")
    
    class_counts = nutrition_df['class'].value_counts()
    
    # Create pie chart
    plt.figure(figsize=(12, 8))
    colors = sns.color_palette("husl", len(class_counts))
    plt.pie(class_counts.values, labels=class_counts.index, autopct='%1.1f%%',
            startangle=90, colors=colors, textprops={'fontsize': 10})
    plt.title('Distribution of Health Categories', fontsize=14, fontweight='bold', pad=20)
    plt.tight_layout()
    plt.savefig('visualizations/health_categories_pie.png', dpi=300, bbox_inches='tight')
    print("  ✓ Saved: visualizations/health_categories_pie.png")
    plt.close()
    
    # Bar chart version
    plt.figure(figsize=(14, 6))
    ax = class_counts.plot(kind='bar', color='mediumseagreen')
    plt.xlabel('Health Category', fontsize=12)
    plt.ylabel('Number of Records', fontsize=12)
    plt.title('Health Categories - Record Count', fontsize=14, fontweight='bold', pad=20)
    plt.xticks(rotation=45, ha='right')
    
    # Add value labels
    for i, v in enumerate(class_counts.values):
        ax.text(i, v + max(class_counts.values)*0.01, f'{v:,}', 
               ha='center', fontsize=10)
    
    plt.tight_layout()
    plt.savefig('visualizations/health_categories_bar.png', dpi=300, bbox_inches='tight')
    print("  ✓ Saved: visualizations/health_categories_bar.png")
    plt.close()

# ============================================================
# VISUALIZATION 4: TIME SERIES
# ============================================================

if 'yearstart' in nutrition_df.columns and 'data_value' in nutrition_df.columns:
    print("\n4. Health Trends Over Time:")
    
    # Overall trend
    yearly_data = nutrition_df.groupby('yearstart').agg({
        'data_value': ['mean', 'median', 'std']
    }).reset_index()
    yearly_data.columns = ['year', 'mean', 'median', 'std']
    
    plt.figure(figsize=(14, 6))
    plt.plot(yearly_data['year'], yearly_data['mean'], marker='o', 
            linewidth=2.5, markersize=8, label='Mean', color='steelblue')
    plt.plot(yearly_data['year'], yearly_data['median'], marker='s', 
            linewidth=2.5, markersize=8, label='Median', color='coral')
    
    # Add confidence interval
    plt.fill_between(yearly_data['year'], 
                     yearly_data['mean'] - yearly_data['std'],
                     yearly_data['mean'] + yearly_data['std'],
                     alpha=0.2, color='steelblue', label='±1 Std Dev')
    
    plt.xlabel('Year', fontsize=12)
    plt.ylabel('Health Indicator Value', fontsize=12)
    plt.title('Health Indicators Trends Over Time', fontsize=14, fontweight='bold', pad=20)
    plt.legend(fontsize=11)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig('visualizations/health_trends_time_series.png', dpi=300, bbox_inches='tight')
    print("  ✓ Saved: visualizations/health_trends_time_series.png")
    plt.close()
    
    # By category over time
    if 'class' in nutrition_df.columns:
        print("\n5. Health Trends by Category:")
        
        category_time = nutrition_df.groupby(['yearstart', 'class'])['data_value'].mean().reset_index()
        top_categories = nutrition_df['class'].value_counts().head(5).index
        
        plt.figure(figsize=(14, 8))
        for category in top_categories:
            cat_data = category_time[category_time['class'] == category]
            plt.plot(cat_data['yearstart'], cat_data['data_value'], 
                    marker='o', linewidth=2, markersize=6, label=category)
        
        plt.xlabel('Year', fontsize=12)
        plt.ylabel('Average Value', fontsize=12)
        plt.title('Health Trends by Category Over Time', fontsize=14, fontweight='bold', pad=20)
        plt.legend(fontsize=10, loc='best')
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig('visualizations/health_trends_by_category.png', dpi=300, bbox_inches='tight')
        print("  ✓ Saved: visualizations/health_trends_by_category.png")
        plt.close()

# ============================================================
# VISUALIZATION 5: DISTRIBUTION PLOTS
# ============================================================

if 'data_value' in nutrition_df.columns:
    print("\n6. Data Value Distribution:")
    
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    
    # Histogram
    axes[0].hist(nutrition_df['data_value'].dropna(), bins=50, 
                color='steelblue', edgecolor='black', alpha=0.7)
    axes[0].set_xlabel('Data Value', fontsize=12)
    axes[0].set_ylabel('Frequency', fontsize=12)
    axes[0].set_title('Distribution of Health Indicator Values', 
                     fontsize=13, fontweight='bold')
    axes[0].grid(True, alpha=0.3)
    
    # Box plot by class
    if 'class' in nutrition_df.columns:
        top_classes = nutrition_df['class'].value_counts().head(5).index
        data_for_box = nutrition_df[nutrition_df['class'].isin(top_classes)]
        
        data_for_box.boxplot(column='data_value', by='class', ax=axes[1])
        axes[1].set_xlabel('Health Category', fontsize=12)
        axes[1].set_ylabel('Data Value', fontsize=12)
        axes[1].set_title('Value Distribution by Category', 
                         fontsize=13, fontweight='bold')
        plt.sca(axes[1])
        plt.xticks(rotation=45, ha='right')
    
    plt.suptitle('')  # Remove default title
    plt.tight_layout()
    plt.savefig('visualizations/data_value_distributions.png', dpi=300, bbox_inches='tight')
    print("  ✓ Saved: visualizations/data_value_distributions.png")
    plt.close()

# ============================================================
# VISUALIZATION 6: SAMPLE SIZE ANALYSIS
# ============================================================

if 'sample_size' in nutrition_df.columns and 'data_value' in nutrition_df.columns:
    print("\n7. Sample Size vs Data Value:")
    
    plt.figure(figsize=(12, 6))
    plt.scatter(nutrition_df['sample_size'], nutrition_df['data_value'], 
               alpha=0.5, s=30, color='mediumseagreen')
    plt.xlabel('Sample Size', fontsize=12)
    plt.ylabel('Data Value', fontsize=12)
    plt.title('Relationship between Sample Size and Data Value', 
             fontsize=14, fontweight='bold', pad=20)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig('visualizations/sample_size_vs_value.png', dpi=300, bbox_inches='tight')
    print("  ✓ Saved: visualizations/sample_size_vs_value.png")
    plt.close()

# ============================================================
# SUMMARY
# ============================================================

print("\n" + "="*60)
print("VISUALIZATION SUMMARY")
print("="*60)

viz_files = [f for f in os.listdir('visualizations') if f.endswith('.png')]
print(f"\n✓ Total visualizations created: {len(viz_files)}")
print(f"\n📊 Saved visualizations:")
for i, file in enumerate(viz_files, 1):
    print(f"  {i}. {file}")

print(f"\n✓ All visualizations saved to: 'visualizations/' directory")
print("\n" + "="*60)
print("✓ ANALYSIS AND VISUALIZATION COMPLETE!")
print("="*60)

print("\n📝 Next Steps:")
print("  1. Review all charts in 'visualizations/' folder")
print("  2. Include these in your project report")
print("  3. Optional: Create Streamlit dashboard for interactivity")

# Close connection
client.close()
print("\n✓ MongoDB connection closed.")
