# ETL with Airflow, Docker, and PostgreSQL

A fully working, minimal ETL pipeline using Apache Airflow, Docker, and PostgreSQL. This repository serves as a professional template for real-world data workflows and can be easily adapted to fit your own use case.

## ✨ What You'll Get

* A complete ETL pipeline orchestrated by **Apache Airflow**
* Modular **Docker Compose** setup
* Two PostgreSQL databases: one for Airflow metadata and one for actual data storage
* Volumes and environment variable management using `.env`
* Python scripts for extraction and transformation
* SQL scripts for aggregation and analysis
* Production-ready folder structure

## 🚀 Use Case

Simulated data from a sports retail chain:

* **Extract** data from a 30,000-line JSON file
* **Transform** it into normalized relational tables
* **Load** the data into PostgreSQL
* **Aggregate & Analyze** using SQL views for downstream use (BI, ML, reports, etc.)

## 📁 Project Structure

```
project_root/
├── dags/
├── data/
├── scripts/
├── config/
├── plugins/
├── logs/
├── docker-compose.yaml
├── requirements.txt
└── .env
```

## 🔐 Environment Variables

All sensitive and environment-specific configs are managed in a `.env` file at the project root:

```env
AIRFLOW_IMAGE_NAME=apache/airflow:2.10.5
AIRFLOW_UID=50000
POSTGRES_USER=admin_postgres
POSTGRES_PASSWORD=admin_password
_AIRFLOW_WWW_USER_USERNAME=admin_airflow
_AIRFLOW_WWW_USER_PASSWORD=admin_password
```

## 🪨 Docker Volumes & Bind Mounts

Docker containers are isolated and do not have access to your local file system unless explicitly mapped via volumes.

This project uses **bind mounts** to share source code and data with containers:

```yaml
volumes:
  - ${AIRFLOW_PROJ_DIR:-.}/dags:/opt/airflow/dags
  - ${AIRFLOW_PROJ_DIR:-.}/logs:/opt/airflow/logs
  - ${AIRFLOW_PROJ_DIR:-.}/config:/opt/airflow/config
  - ${AIRFLOW_PROJ_DIR:-.}/plugins:/opt/airflow/plugins
  - ${AIRFLOW_PROJ_DIR:-.}/scripts:/opt/airflow/scripts
  - ${AIRFLOW_PROJ_DIR:-.}/data:/opt/airflow/data
```

## 🌐 Services Overview

This project includes the following services managed by Docker Compose:

* `airflow-webserver`: Airflow UI (localhost:8080)
* `airflow-scheduler`, `worker`, `triggerer`, `flower`, `cli`, `init`: core Airflow services
* `postgres`: PostgreSQL for Airflow metadata
* `redis`: Required for Airflow queues
* `postgres_destination_server`: Target PostgreSQL DB (exposed on port 5433)

## 🎓 Best Practices Followed

* ✅ Persistent volumes to avoid data loss
* ✅ `.env` file to manage secrets/configs
* ✅ Use of service names (e.g., `host=postgres`) within Docker network
* ✅ Custom scripts and SQL files run at container startup

## ⚙️ Running the Project

### 1. Clone the repository

```bash
git clone <repo_url>
cd project_root
```

### 2. Create your `.env` file

```bash
cp .env.example .env
# Edit with your credentials and preferences
```

### 3. Start the environment

```bash
docker-compose up --build
```

### 4. Access the Airflow UI

Navigate to: [http://localhost:8080](http://localhost:8080)

Login with:

* **Username:** as defined in `_AIRFLOW_WWW_USER_USERNAME`
* **Password:** as defined in `_AIRFLOW_WWW_USER_PASSWORD`

### 5. Connect to the PostgreSQL destination DB (optional)

* **Host:** `localhost`
* **Port:** `5433`
* **Username:** as defined in `.env`
* **Password:** as defined in `.env`

Use pgAdmin, DBeaver, or any SQL tool.

## 🌟 Credits

This template was developed as part of a professional training/tutorial to help Data Engineers and Data Scientists quickly bootstrap production-grade ETL projects with modern tools.

## 📈 Future Improvements

* Add unit tests for Python scripts
* Add Airflow Connections and Variables via CLI or env
* CI/CD pipeline for deployment

---

Feel free to fork this repo and adapt it for your own data projects. Contributions welcome!

---

MIT License
