# Airflow Data Transformation Pipeline

This project demonstrates a complete **end‑to‑end ETL pipeline using Apache Airflow and PostgreSQL**, running inside Docker.

The pipeline:

* Reads raw employee data from PostgreSQL
* Applies transformations using Pandas
* Loads clean, enriched data into a transformed table

---

## 📂 Project Structure

```
project-root/
├── docker-compose.yml
├── requirements.txt
├── README.md
├── dags/
│   ├── dag1_csv_to_postgres.py
│   ├── dag2_data_transformation.py
│   ├── dag3_postgres_to_parquet.py
│   ├── dag4_conditional_workflow.py
│   └── dag5_notification_workflow.py
├── tests/
│   ├── test_dag1.py
│   ├── test_dag2.py
│   └── test_utils.py
├── data/
│   └── input.csv
├── output/
│   └── (parquet files will be generated here)
└── plugins/
    └── (optional custom operators or hooks)

```

---

## 🛠️ Tech Stack

* **Apache Airflow** – Workflow orchestration
* **PostgreSQL** – Source & target database
* **Docker & Docker Compose** – Containerization
* **Python (Pandas, SQLAlchemy)** – Data transformation

---

## 🗄️ Database Schema

### Source Table: `raw_employee_data`

| Column   | Type    |
| -------- | ------- |
| id       | INT     |
| name     | TEXT    |
| age      | INT     |
| city     | TEXT    |
| salary   | NUMERIC |
| joindate | DATE    |

### Target Table: `transformed_employee_data`

| Column          | Description                  |
| --------------- | ---------------------------- |
| id              | Employee ID                  |
| name            | Employee name                |
| age             | Age                          |
| city            | City                         |
| salary          | Salary                       |
| joindate        | Join date                    |
| full_info       | `name - city`                |
| age_group       | Young / Mid / Senior         |
| salary_category | Low / Medium / High          |
| year_joined     | Year extracted from joindate |

---

## 🔄 DAG Workflow

**DAG ID:** `data_transformation_pipeline_v2`

### Tasks

1. **create_transformed_table**

   * Creates the target table if it does not exist

2. **transform_and_load**

   * Reads data from `raw_employee_data`
   * Applies transformations
   * Loads data into `transformed_employee_data`

Task dependency:

```
create_transformed_table >> transform_and_load
```

---

## ⚙️ Setup & Run Instructions

### 0️⃣ Clone the Repository (PowerShell)

```powershell
git clone <https://github.com/Kusubhavani/data-processing-workflows>
cd data-processing-workflow
```

### 1️⃣ Start Airflow & Postgres

```bash
docker-compose up -d
```

---

### 2️⃣ Verify Postgres Access

```bash
docker exec -it postgres psql -U airflow_user -d airflow_db
```

---

### 3️⃣ (One‑time) Create Target Table Manually (if needed)

```sql
DROP TABLE IF EXISTS public.transformed_employee_data;

CREATE TABLE public.transformed_employee_data (
    id INT PRIMARY KEY,
    name TEXT,
    age INT,
    city TEXT,
    salary NUMERIC,
    joindate DATE,
    full_info TEXT,
    age_group TEXT,
    salary_category TEXT,
    year_joined INT
);
```

---

### 4️⃣ Clear & Run the DAG

```bash
docker exec -it airflow-webserver airflow dags clear data_transformation_pipeline_v2 --yes
```

```bash
docker exec -it airflow-webserver airflow dags trigger data_transformation_pipeline_v2
```

---

### 5️⃣ Verify Output Data

```sql
SELECT COUNT(*) FROM transformed_employee_data;
SELECT * FROM transformed_employee_data LIMIT 5;
```

---

## 🔁 Restart All DAGs (Optional)

```bash
docker exec -it airflow-webserver airflow dags clear --all --yes
```

```bash
docker exec -it airflow-webserver airflow dags trigger --all
```
Open http://localhost:8081
Login with admin / admin
Enable DAGs
Trigger DAGs manually
Monitor via Graph, Grid, and Logs
FINAL STATUS
All DAGs executed successfully:

DAG 1 ✅
DAG 2 ✅
DAG 3 ✅
DAG 4 ✅
DAG 5 ✅
---

## 🚀 Key Learnings

* Airflow does **not auto‑migrate database schemas**
* Docker Postgres creates **only one default user**
* DAG fixes require **clearing old DAG runs**
* Column name mismatches are a common ETL failure cause

---

## 📌 Notes

* Airflow connection `postgres_default` must match Postgres container credentials
* All commands assume Docker container names:

  * `airflow-webserver`
  * `postgres`

---

## ✅ Status

✔ DAG running successfully
✔ Data loaded correctly
✔ Production‑ready ETL pattern

---

**Author:** Bhavani
