# Synthetica AI Big Data Engineer Case Study

## End-to-End Data Pipeline

This project is the deliverable for Exercise 12 of the Synthetica AI Big Data Engineer Case Study.
# Synthetica AI Big Data Engineer Case Study

## End-to-End Data Pipeline

This project is the deliverable for Exercise 12 of the Synthetica AI Big Data Engineer Case Study.

It implements an **end-to-end data pipeline**, containerized and deployed on **Kubernetes with Minikube**, using:

* **PySpark** for data processing and advanced analytics
* **PostgreSQL** as the data warehouse
* **Async Python** for ingestion and cleaning
* **Docker** to containerize the pipeline
* **Kubernetes** for orchestration
* **pgAdmin** for data warehouse browsing

---

## Architecture

```
+----------------+      +------------------+      +-----------------------+
| Data Sources   | ---> | Async Cleaning    | ---> | PostgreSQL DWH        |
| (simulated CSV)|      | (Parallel chunks) |      | (DIM, FACT, MARTS)    |
+----------------+      +------------------+      +-----------------------+
       |                        |                            |
       v                        v                            v
  data/data_sources/       data/staging_area/            pgAdmin, output/logs/
```

### Pipeline Steps

1. **Ingestion:** Simulated multiple source ingestion → splits CSV into chunks
2. **Cleaning:** Async cleaning of chunks → standardizes and deduplicates
3. **Database Initialization:** Drops and recreates DWH → creates DIM, FACT tables
4. **Transformation:** Spark pipeline loads data, populates DWH tables
5. **Analytics:** Final data marts created in DWH
6. **Results:** Logs and data files are written to container filesystem → copied out via `kubectl cp`

---

## Approach

To address the case study, I followed these principles:

* Designed the pipeline to be **fully repeatable** → the database is reset each run.
* Implemented **async ingestion and cleaning** to showcase parallel processing.
* Used **PySpark SparkSQL + UDFs** for advanced data transformations and aggregations.
* Structured the DWH as a **star schema** (fact table + dimension tables) and added data marts.
* Ensured the pipeline is **containerized and Kubernetes-ready**, matching Task 12 requirements.
* Provided clear instructions on **how to run and inspect the pipeline**.

---

## Running the Pipeline

### Prerequisites

* Minikube installed and running
* Docker installed and configured to push to DockerHub
* Docker image `aandrio/pipeline_app:latest` pushed to DockerHub

### Build and Push Docker Image

```bash
docker build -t aandrio/pipeline_app:latest .
docker push aandrio/pipeline_app:latest
```

### Start Minikube

```bash
minikube start
```

---

## Kubernetes Deployment

### 1️⃣ Deploy Postgres

```bash
kubectl apply -f k8s/postgres-deployment.yaml
kubectl apply -f k8s/postgres-service.yaml
```

### 2️⃣ Deploy pgAdmin (optional but recommended)

```bash
kubectl apply -f k8s/pgadmin-deployment.yaml
kubectl apply -f k8s/pgadmin-service.yaml
```

Access pgAdmin at: [http://localhost:30800](http://localhost:30800)
Login: `synthetica_user` / `synthetica_pass`
Add connection:

* Host: `postgres`
* Port: `5432`
* DB: `acs_dataset`
* User: `synthetica_user`
* Pass: `synthetica_pass`

### 3️⃣ Run the Pipeline Job

```bash
kubectl apply -f k8s/pipeline-job.yaml
```

### 4️⃣ Monitor Pipeline Job

```bash
kubectl get jobs
kubectl logs job/pipeline-job
```

### 5️⃣ Copy Results (Logs and Data)

```bash
# Find the pod name:
kubectl get pods

# Example:
# pipeline-job-xxxxx-xxxxx

# Copy output/logs:
kubectl cp pipeline-job-xxxxx-xxxxx:/app/output/logs ./output/logs

# Copy data:
kubectl cp pipeline-job-xxxxx-xxxxx:/app/data ./data
```

### 6️⃣ View Results

* Logs will be in `./output/logs/`
* Data files will be in `./data/`
* Database tables can be viewed in pgAdmin:

  * DIM tables
  * FACT table
  * MARTS

---

## Notes

* The pipeline is **idempotent** → it can be safely run multiple times.
* The PostgreSQL database is reset on each run → no persistent database storage required.
* The project uses a **star schema** + data marts → fully aligned with data warehousing best practices.

## Schema

* The database schema is provided in `.schema/acs_dataset_schema - dbeaver.png`.

---

## Closing Remarks

This repository contains the full deliverable for **Exercise 12** of the case study.

It demonstrates:

* SparkSQL + UDFs
* Async Python
* Containerization with Docker
* Kubernetes orchestration with Minikube
* Proper DWH modeling (star schema)

The pipeline can be cloned, built, deployed and run fully locally with Minikube.

**Important:** This `README.md` serves as the main documentation for the repository, per submission requirements.

Thank you for your time and consideration.

It implements an **end-to-end data pipeline**, containerized and deployed on **Kubernetes with Minikube**, using:

* **PySpark** for data processing and advanced analytics
* **PostgreSQL** as the data warehouse
* **Async Python** for ingestion and cleaning
* **Docker** to containerize the pipeline
* **Kubernetes** for orchestration
* **pgAdmin** for data warehouse browsing

---

## Architecture

```
+----------------+      +------------------+      +-----------------------+
| Data Sources   | ---> | Async Cleaning    | ---> | PostgreSQL DWH        |
| (simulated CSV)|      | (Parallel chunks) |      | (DIM, FACT, MARTS)    |
+----------------+      +------------------+      +-----------------------+
       |                        |                            |
       v                        v                            v
  data/data_sources/       data/staging_area/            pgAdmin, output/logs/
```

### Pipeline Steps

1. **Ingestion:** Simulated multiple source ingestion → splits CSV into chunks
2. **Cleaning:** Async cleaning of chunks → standardizes and deduplicates
3. **Database Initialization:** Drops and recreates DWH → creates DIM, FACT tables
4. **Transformation:** Spark pipeline loads data, populates DWH tables
5. **Analytics:** Final data marts created in DWH
6. **Results:** Logs and data files are written to container filesystem → copied out via `kubectl cp`

---

## Approach

To address the case study, I followed these principles:

* Designed the pipeline to be **fully repeatable** → the database is reset each run.
* Implemented **async ingestion and cleaning** to showcase parallel processing.
* Used **PySpark SparkSQL + UDFs** for advanced data transformations and aggregations.
* Structured the DWH as a **star schema** (fact table + dimension tables) and added data marts.
* Ensured the pipeline is **containerized and Kubernetes-ready**, matching Task 12 requirements.
* Provided clear instructions on **how to run and inspect the pipeline**.

---

## Running the Pipeline

### Prerequisites

* Minikube installed and running
* Docker installed and configured to push to DockerHub
* Docker image `aandrio/pipeline_app:latest` pushed to DockerHub

### Build and Push Docker Image

```bash
docker build -t aandrio/pipeline_app:latest .
docker push aandrio/pipeline_app:latest
```

### Start Minikube

```bash
minikube start
```

---

## Kubernetes Deployment

### 1️⃣ Deploy Postgres

```bash
kubectl apply -f k8s/postgres-deployment.yaml
kubectl apply -f k8s/postgres-service.yaml
```

### 2️⃣ Deploy pgAdmin (optional but recommended)

```bash
kubectl apply -f k8s/pgadmin-deployment.yaml
kubectl apply -f k8s/pgadmin-service.yaml
```

Access pgAdmin at: [http://localhost:30800](http://localhost:30800)
Login: `synthetica_user` / `synthetica_pass`
Add connection:

* Host: `postgres`
* Port: `5432`
* DB: `acs_dataset`
* User: `synthetica_user`
* Pass: `synthetica_pass`

### 3️⃣ Run the Pipeline Job

```bash
kubectl apply -f k8s/pipeline-job.yaml
```

### 4️⃣ Monitor Pipeline Job

```bash
kubectl get jobs
kubectl logs job/pipeline-job
```

### 5️⃣ Copy Results (Logs and Data)

```bash
# Find the pod name:
kubectl get pods

# Example:
# pipeline-job-xxxxx-xxxxx

# Copy output/logs:
kubectl cp pipeline-job-xxxxx-xxxxx:/app/output/logs ./output/logs

# Copy data:
kubectl cp pipeline-job-xxxxx-xxxxx:/app/data ./data
```

### 6️⃣ View Results

* Logs will be in `./output/logs/`
* Data files will be in `./data/`
* Database tables can be viewed in pgAdmin:

  * DIM tables
  * FACT table
  * MARTS

---

## Notes

* The pipeline is **idempotent** → it can be safely run multiple times.
* The PostgreSQL database is reset on each run → no persistent database storage required.
* The project uses a **star schema** + data marts → fully aligned with data warehousing best practices.

## Schema

* The database schema is provided in `.schema/acs_dataset_schema - dbeaver.png`.

---

## Closing Remarks

This repository contains the full deliverable for **Exercise 12** of the case study.

It demonstrates:

* SparkSQL + UDFs
* Async Python
* Containerization with Docker
* Kubernetes orchestration with Minikube
* Proper DWH modeling (star schema)

The pipeline can be cloned, built, deployed and run fully locally with Minikube.

**Important:** This `README.md` serves as the main documentation for the repository, per submission requirements.

Thank you for your time and consideration.
# Synthetica AI Big Data Engineer Case Study

## End-to-End Data Pipeline

This project is the deliverable for Exercise 12 of the Synthetica AI Big Data Engineer Case Study.

It implements an **end-to-end data pipeline**, containerized and deployed on **Kubernetes with Minikube**, using:

* **PySpark** for data processing and advanced analytics
* **PostgreSQL** as the data warehouse
* **Async Python** for ingestion and cleaning
* **Docker** to containerize the pipeline
* **Kubernetes** for orchestration
* **pgAdmin** for data warehouse browsing

---

## Architecture

```
+----------------+      +------------------+      +-----------------------+
| Data Sources   | ---> | Async Cleaning    | ---> | PostgreSQL DWH        |
| (simulated CSV)|      | (Parallel chunks) |      | (DIM, FACT, MARTS)    |
+----------------+      +------------------+      +-----------------------+
       |                        |                            |
       v                        v                            v
  data/data_sources/       data/staging_area/            pgAdmin, output/logs/
```

### Pipeline Steps

1. **Ingestion:** Simulated multiple source ingestion → splits CSV into chunks
2. **Cleaning:** Async cleaning of chunks → standardizes and deduplicates
3. **Database Initialization:** Drops and recreates DWH → creates DIM, FACT tables
4. **Transformation:** Spark pipeline loads data, populates DWH tables
5. **Analytics:** Final data marts created in DWH
6. **Results:** Logs and data files are written to container filesystem → copied out via `kubectl cp`

---

## Approach

To address the case study, I followed these principles:

* Designed the pipeline to be **fully repeatable** → the database is reset each run.
* Implemented **async ingestion and cleaning** to showcase parallel processing.
* Used **PySpark SparkSQL + UDFs** for advanced data transformations and aggregations.
* Structured the DWH as a **star schema** (fact table + dimension tables) and added data marts.
* Ensured the pipeline is **containerized and Kubernetes-ready**, matching Task 12 requirements.
* Provided clear instructions on **how to run and inspect the pipeline**.

---

## Running the Pipeline

### Prerequisites

* Minikube installed and running
* Docker installed and configured to push to DockerHub
* Docker image `aandrio/pipeline_app:latest` pushed to DockerHub

### Build and Push Docker Image

```bash
docker build -t aandrio/pipeline_app:latest .
docker push aandrio/pipeline_app:latest
```

### Start Minikube

```bash
minikube start
```

---

## Kubernetes Deployment

### 1️⃣ Deploy Postgres

```bash
kubectl apply -f k8s/postgres-deployment.yaml
kubectl apply -f k8s/postgres-service.yaml
```

### 2️⃣ Deploy pgAdmin (optional but recommended)

```bash
kubectl apply -f k8s/pgadmin-deployment.yaml
kubectl apply -f k8s/pgadmin-service.yaml
```

Access pgAdmin at: [http://localhost:30800](http://localhost:30800)
Login: `synthetica_user` / `synthetica_pass`
Add connection:

* Host: `postgres`
* Port: `5432`
* DB: `acs_dataset`
* User: `synthetica_user`
* Pass: `synthetica_pass`

### 3️⃣ Run the Pipeline Job

```bash
kubectl apply -f k8s/pipeline-job.yaml
```

### 4️⃣ Monitor Pipeline Job

```bash
kubectl get jobs
kubectl logs job/pipeline-job
```

### 5️⃣ Copy Results (Logs and Data)

```bash
# Find the pod name:
kubectl get pods

# Example:
# pipeline-job-xxxxx-xxxxx

# Copy output/logs:
kubectl cp pipeline-job-xxxxx-xxxxx:/app/output/logs ./output/logs

# Copy data:
kubectl cp pipeline-job-xxxxx-xxxxx:/app/data ./data
```

### 6️⃣ View Results

* Logs will be in `./output/logs/`
* Data files will be in `./data/`
* Database tables can be viewed in pgAdmin:

  * DIM tables
  * FACT table
  * MARTS

---

## Notes

* The pipeline is **idempotent** → it can be safely run multiple times.
* The PostgreSQL database is reset on each run → no persistent database storage required.
* The project uses a **star schema** + data marts → fully aligned with data warehousing best practices.

## Schema

* The database schema is provided in `.schema/acs_dataset_schema - dbeaver.png`.

---

## Closing Remarks

This repository contains the full deliverable for **Exercise 12** of the case study.

It demonstrates:
# Synthetica AI Big Data Engineer Case Study

## End-to-End Data Pipeline

This project is the deliverable for Exercise 12 of the Synthetica AI Big Data Engineer Case Study.

It implements an **end-to-end data pipeline**, containerized and deployed on **Kubernetes with Minikube**, using:

* **PySpark** for data processing and advanced analytics
* **PostgreSQL** as the data warehouse
* **Async Python** for ingestion and cleaning
* **Docker** to containerize the pipeline
* **Kubernetes** for orchestration
* **pgAdmin** for data warehouse browsing

---

## Architecture

```
+----------------+      +------------------+      +-----------------------+
| Data Sources   | ---> | Async Cleaning    | ---> | PostgreSQL DWH        |
| (simulated CSV)|      | (Parallel chunks) |      | (DIM, FACT, MARTS)    |
+----------------+      +------------------+      +-----------------------+
       |                        |                            |
       v                        v                            v
  data/data_sources/       data/staging_area/            pgAdmin, output/logs/
```

### Pipeline Steps

1. **Ingestion:** Simulated multiple source ingestion → splits CSV into chunks
2. **Cleaning:** Async cleaning of chunks → standardizes and deduplicates
3. **Database Initialization:** Drops and recreates DWH → creates DIM, FACT tables
4. **Transformation:** Spark pipeline loads data, populates DWH tables
5. **Analytics:** Final data marts created in DWH
6. **Results:** Logs and data files are written to container filesystem → copied out via `kubectl cp`

---

## Approach

To address the case study, I followed these principles:

* Designed the pipeline to be **fully repeatable** → the database is reset each run.
* Implemented **async ingestion and cleaning** to showcase parallel processing.
* Used **PySpark SparkSQL + UDFs** for advanced data transformations and aggregations.
* Structured the DWH as a **star schema** (fact table + dimension tables) and added data marts.
* Ensured the pipeline is **containerized and Kubernetes-ready**, matching Task 12 requirements.
* Provided clear instructions on **how to run and inspect the pipeline**.

---

## Running the Pipeline

### Prerequisites

* Minikube installed and running
* Docker installed and configured to push to DockerHub
* Docker image `aandrio/pipeline_app:latest` pushed to DockerHub

### Build and Push Docker Image

```bash
docker build -t aandrio/pipeline_app:latest .
docker push aandrio/pipeline_app:latest
```

### Start Minikube

```bash
minikube start
```

---

## Kubernetes Deployment

### 1️⃣ Deploy Postgres

```bash
kubectl apply -f k8s/postgres-deployment.yaml
kubectl apply -f k8s/postgres-service.yaml
```

### 2️⃣ Deploy pgAdmin (optional but recommended)

```bash
kubectl apply -f k8s/pgadmin-deployment.yaml
kubectl apply -f k8s/pgadmin-service.yaml
```

Access pgAdmin at: [http://localhost:30800](http://localhost:30800)
Login: `synthetica_user` / `synthetica_pass`
Add connection:

* Host: `postgres`
* Port: `5432`
* DB: `acs_dataset`
* User: `synthetica_user`
* Pass: `synthetica_pass`

### 3️⃣ Run the Pipeline Job

```bash
kubectl apply -f k8s/pipeline-job.yaml
```

### 4️⃣ Monitor Pipeline Job

```bash
kubectl get jobs
kubectl logs job/pipeline-job
```

### 5️⃣ Copy Results (Logs and Data)

```bash
# Find the pod name:
kubectl get pods

# Example:
# pipeline-job-xxxxx-xxxxx

# Copy output/logs:
kubectl cp pipeline-job-xxxxx-xxxxx:/app/output/logs ./output/logs

# Copy data:
kubectl cp pipeline-job-xxxxx-xxxxx:/app/data ./data
```

### 6️⃣ View Results

* Logs will be in `./output/logs/`
* Data files will be in `./data/`
* Database tables can be viewed in pgAdmin:

  * DIM tables
  * FACT table
  * MARTS

---

## Notes

* The pipeline is **idempotent** → it can be safely run multiple times.
* The PostgreSQL database is reset on each run → no persistent database storage required.
* The project uses a **star schema** + data marts → fully aligned with data warehousing best practices.

## Schema

* The database schema is provided in `.schema/acs_dataset_schema - dbeaver.png`.

---

## Closing Remarks

This repository contains the full deliverable for **Exercise 12** of the case study.

It demonstrates:

* SparkSQL + UDFs
* Async Python
* Containerization with Docker
* Kubernetes orchestration with Minikube
* Proper DWH modeling (star schema)

The pipeline can be cloned, built, deployed and run fully locally with Minikube.

**Important:** This `README.md` serves as the main documentation for the repository, per submission requirements.

Thank you for your time and consideration.

* SparkSQL + UDFs
* Async Python
* Containerization with Docker
* Kubernetes orchestration with Minikube
* Proper DWH modeling (star schema)

The pipeline can be cloned, built, deployed and run fully locally with Minikube.

**Important:** This `README.md` serves as the main documentation for the repository, per submission requirements.

Thank you for your time and consideration.
