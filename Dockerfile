# Final Dockerfile for pipeline_app
FROM python:3.12.3-slim-bullseye

# Install Java runtime for Spark
RUN apt-get update && apt-get install -y openjdk-17-jre-headless && rm -rf /var/lib/apt/lists/*

# Set workdir
WORKDIR /app

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ src/

# Copy csv dataset
COPY data/data_sources/ data/data_sources/

# Copy JDBC driver
COPY jars/postgresql-42.7.3.jar jars/postgresql-42.7.3.jar

# Run main pipeline script
CMD ["python", "src/main.py"]

