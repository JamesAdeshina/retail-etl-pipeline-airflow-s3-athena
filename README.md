#  Retail ETL Pipeline: PostgreSQL to AWS with Airflow & Athena

[![Python](https://img.shields.io/badge/Python-3.10-blue)](https://www.python.org/)
[![Airflow](https://img.shields.io/badge/Airflow-2.7.0-green)](https://airflow.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue)](https://www.postgresql.org/)
[![AWS](https://img.shields.io/badge/AWS-S3%2C%20Athena-orange)](https://aws.amazon.com/)

An end-to-end ETL pipeline migrating on-premise PostgreSQL retail transactional data to AWS cloud, implementing a Medallion Architecture (Bronze/Silver/Gold) for scalable analytics.

##  Business Context
ShopEase Retail Ltd. needed to migrate from slow, on-premise PostgreSQL reporting to a scalable cloud analytics solution. This pipeline enables secure, performant, and cost-effective access to sales, customer, and inventory data for business intelligence.

##  Architecture

On-Prem PostgreSQL → Airflow (Docker) → AWS S3 (Bronze/Silver/Gold) → Athena → BI Dashboards


### Tech Stack
- **Orchestration**: Apache Airflow 2.7.0
- **Source**: PostgreSQL 15 (Dockerized)
- **Storage**: AWS S3 with Medallion Architecture
- **Processing**: AWS Athena, AWS Glue
- **Transformation**: Python, Pandas, AWS Data Wrangler
- **Containerization**: Docker, Docker Compose

##  Project Structure
```
retail-etl-pipeline-airflow-s3-athena/
├── dags/ # Airflow DAG definitions
├── scripts/ # Python ETL scripts
├── docker/ # Docker configuration
├── config/ # Configuration files
├── sql/ # Athena query templates
├── docs/ # Documentation
└── data/ # Sample/data files (gitignored)
```


##  Quick Start

### 1. Prerequisites
- Docker & Docker Compose
- AWS Account (for S3/Athena)
- Python 3.10+

### 2. Local Setup
```bash
# Clone repository
git clone <your-repo-url>
cd retail-etl-pipeline-airflow-s3-athena

# Start PostgreSQL with sample data
docker run -d --name shopease_postgres -p 5433:5432 \
  -e POSTGRES_PASSWORD=shopease123 postgres:15

# Start Airflow
cd docker
docker-compose up -d