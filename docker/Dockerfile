# Spark + Python image for running the ETL pipeline in a reproducible environment.
# Mirrors what would run on an EMR step or as a containerized Glue job.
FROM python:3.11-slim

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PYSPARK_PYTHON=python3 \
    PYTHONUNBUFFERED=1

RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY run_pipeline.py .
COPY tests/ ./tests/

# Default: generate sample data and run the pipeline end to end.
# Override the command to run tests instead: docker run <image> pytest tests/ -v
CMD ["sh", "-c", "python src/ingestion/generate_events.py --output-dir data/sample && python run_pipeline.py"]
