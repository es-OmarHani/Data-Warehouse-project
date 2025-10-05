# 🏗️ Data Warehouse Project (CRM & ERP Integration)

This project implements a **Data Warehouse (DWH)** architecture using **Microsoft SQL Server**, structured into three main layers — **Bronze**, **Silver**, and **Gold**.  

The goal is to integrate CRM and ERP data sources, clean and standardize the information, and expose business-ready data models for analytics.

---

## 📘 Overview of Layers

| Layer | Description |
|--------|-------------|
| **Bronze** | Raw data ingestion from CRM and ERP CSV sources using `BULK INSERT`. |
| **Silver** | Cleansed, standardized, and deduplicated version of the Bronze data. |
| **Gold** | Final analytical **views** representing a **logical Star Schema** (fact and dimension relationships) for BI and reporting. |

---

## ⚙️ Execution Flow

Run the SQL scripts in the following logical order:

### 1️⃣ Load Bronze Layer
Executes the stored procedure responsible for reading CSV files into raw staging tables.

```sql
EXEC bronze.load_bronze;
```

### 2️⃣ Transform and Load Silver Layer
Cleans and standardizes the Bronze layer data.

```sql
EXEC silver.load_silver;
```

---

### 3️⃣ Create Gold Layer Views
Generates business-ready **SQL Views** that logically represent the **Star Schema** (Fact & Dimension design).  
> ⚠️ Note: In the Gold layer, we **created SQL views**, not physical tables. These views represent the **logical structure** of a star schema (Fact & Dimension).

Example command:
```sql
-- Example: Create all analytical views
scripts/gold/01_create_gold_views.sql
```

---

### 🧩 Data Flow Summary
```
CSV Files → Bronze Tables → Silver Tables → Gold Views
```

---

### 🥉 Bronze Layer
Loads **raw CSV data** from CRM and ERP systems using `BULK INSERT`.

- No transformations or cleaning — acts purely as a **data landing zone**.
- Provides traceability and ensures reproducibility for ingestion.

**Example:**
```sql
EXEC bronze.load_bronze;
```

---

### 🥈 Silver Layer
Cleans and transforms the Bronze data by:

- Removing duplicates  
- Fixing missing or invalid values  
- Standardizing categorical codes (e.g., gender, marital status)  
- Normalizing IDs and correcting inconsistent data formats  

**Result:** Clean, consistent, and analysis-ready tables.

**Example:**
```sql
EXEC silver.load_silver;
```

---

### 🥇 Gold Layer
Contains **SQL Views**, *not physical tables*.  

These views:
- Represent a **logical Star Schema**, combining Silver tables into **Fact** and **Dimension** structures for analytics.  
- Enable direct querying from **BI tools** such as Power BI, Tableau, or Excel.  
- Allow flexible analysis without duplicating or materializing data.

---

### ⭐ Example Gold Views

| View Name | Description |
|------------|-------------|
| `gold.v_dim_customers` | Combines CRM and ERP customer data into a unified customer dimension. |
| `gold.v_dim_products` | Consolidates product details and categories into a clean dimension view. |
| `gold.v_fact_sales` | Represents transactional sales data joined with dimensions for analytics. |

**Example Queries:**
```sql
SELECT TOP 10 * FROM gold.v_fact_sales;
SELECT TOP 10 * FROM gold.v_dim_customers;
```

---

### ⚠️ Notes
- Ensure **SQL Server** has **read access** to your CSV folder path (used in `BULK INSERT`).
- Modify `@base_path` inside the **Bronze load procedure** to match your **local dataset path**.
- The **Gold Layer** does **not** store data physically — it **exposes logical views** joining and aggregating Silver data dynamically.
