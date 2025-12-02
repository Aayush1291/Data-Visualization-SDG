# **📊 Data Analytics & Visualization Project — SDG Analysis**

A complete end-to-end **data analytics and visualization project** focused on Electric Vehicle adoption and Health/Nutrition indicators, aligned with United Nations **Sustainable Development Goals (SDGs)**.
This project includes data ingestion, cleaning, transformation, EDA, and professional visualizations.

---

## **📚 Table of Contents**

* [Overview](#overview)
* [SDG Goals Covered](#sdg-goals-covered)
* [Technologies Used](#technologies-used)
* [Prerequisites](#prerequisites)
* [Installation & Setup](#installation--setup)
* [Project Structure](#project-structure)
* [How to Run](#how-to-run)
* [Datasets](#datasets)
* [Expected Outputs](#expected-outputs)
* [Author](#author)
* [Troubleshooting](#troubleshooting)
* [License](#license)

---

## **🎯 Overview**

This project demonstrates a real-world data analytics workflow using two public datasets:

### **1️⃣ Electric Vehicle Population Data (Washington State)**

**SDGs:**

* Goal 7 — Affordable & Clean Energy
* Goal 11 — Sustainable Cities
* Goal 13 — Climate Action

### **2️⃣ Nutrition, Physical Activity & Obesity Data (CDC)**

**SDGs:**

* Goal 2 — Zero Hunger
* Goal 3 — Good Health & Well-Being

### The project includes:

✔ REST API ingestion
✔ MongoDB NoSQL storage
✔ Data cleaning & preprocessing
✔ Exploratory Data Analysis (EDA)
✔ Professional charts & dashboards
✔ CSV exports

---

## **🌍 SDG Goals Covered**

| Dataset           | SDG Goals |
| ----------------- | --------- |
| Electric Vehicles | 7, 11, 13 |
| Nutrition/Health  | 2, 3      |

---

## **🛠 Technologies Used**

* **Python 3.8+**
* **MongoDB Atlas**
* **Libraries**

  * `pymongo`
  * `pandas`
  * `numpy`
  * `matplotlib`
  * `seaborn`
  * `requests`
  * `python-dotenv`
  * `dnspython`

---

## **📦 Prerequisites**

* Python 3.8 or higher
* MongoDB Atlas account
* Internet connection for API ingestion

---
## **Cloning the Project**

```bash
git clone https://github.com/Aayush1291/Akku.git
```
---

## **🚀 Installation & Setup**

### **1. Create a Virtual Environment**

**Windows**

```bash
python -m venv env
env\Scripts\activate
```

**macOS/Linux**

```bash
python3 -m venv env
source env/bin/activate
```

---

### **2. Install Dependencies**

```bash
pip install -r requirements.txt
```

---

### **3. Configure MongoDB Environment Variables**

Create `.env` file:

```
# MongoDB Config
MONGO_PASSWORD=your_password_here
MONGO_CONNECTION_STRING=mongodb+srv://username:<db_password>@cluster.mongodb.net/?retryWrites=true&w=majority
MONGO_DB_NAME=AkkuProject
```

⚠ Replace placeholders with valid credentials.

---

## **📁 Project Structure**

```
Akkuproject/
│
├── .env
├── requirements.txt
├── README.md
│
├── 1_data_ingestion.py
├── 2_data_cleaning.py
├── 3_data_analysis.py
├── 4_visualizations.py
│
├── ev_data_cleaned.csv
├── nutrition_data_cleaned.csv
│
├── analysis_outputs/
│   ├── nutrition_correlation_heatmap.png
│   └── health_trends_over_time.png
│
└── visualizations/
    ├── ev_distribution_*.png
    ├── health_by_state.png
    ├── health_categories_pie.png
    ├── health_trends_time_series.png
    ├── data_value_distributions.png
    └── sample_size_vs_value.png
```

---

## **▶️ How to Run**

### **1️⃣ Run Data Ingestion**

Fetches 2,500 records per dataset from public APIs.

```bash
python 1_data_ingestion.py
```

---

### **2️⃣ Run Data Cleaning**

Cleans missing values, formats columns, exports CSV.

```bash
python 2_data_cleaning.py
```

---

### **3️⃣ Run Exploratory Data Analysis**

Generates initial statistical and trend analyses.

```bash
python 3_data_analysis.py
```

---

### **4️⃣ Run Visualizations**

Creates professional-grade figures for your report.

```bash
python 4_visualizations.py
```

---

## **📊 Datasets**

### **Electric Vehicle Population Data**

* Source: Washington State
* API: `https://data.wa.gov/api/views/f6w7-q2d2/rows.json`
* Records: 2,500 (sampled)

### **Nutrition / Health / Obesity Data**

* Source: CDC
* API: `https://chronicdata.cdc.gov/resource/hn4x-zwk7.json`
* Records: 2,500

---

## **📈 Expected Outputs**

### **MongoDB Collections**

* `ev_data_raw`
* `nutrition_data_raw`
* `ev_data_cleaned`
* `nutrition_data_cleaned`

### **CSV Files**

* `ev_data_cleaned.csv`
* `nutrition_data_cleaned.csv`

### **Visualizations**

* EV adoption charts
* Correlation heatmaps
* State-wise health comparisons
* Obesity trend lines
* Category bar & pie charts
* Scatter plots & distribution plots

---

## **👨‍💻 Author**

**Aayush Balip**
AP Shah Institute of Technology
Mumbai, India

Course: **Data Analytics & Visualization**
Date: **December 2025**

---

## **⚠ Troubleshooting**

| Issue                                         | Cause                           | Fix                                   |
| --------------------------------------------- | ------------------------------- | ------------------------------------- |
| `ModuleNotFoundError`                         | Missing dependencies            | Run `pip install -r requirements.txt` |
| MongoDB connection error                      | Wrong credentials or blocked IP | Update `.env`, whitelist IP in Atlas  |
| API Timeout                                   | Network issue                   | Retry after a few seconds             |
| “Database already exists with different case” | Case-sensitive DB name          | Use: `AkkuProject`                    |

---

## **📄 License**

This project is created for educational use as part of academic coursework.

---
