FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 \
    SERVERS_CONFIG_PATH=/etc/collect-metrics/config.yaml
WORKDIR /app

# Install system dependencies (if any) and Python libraries
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
  && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your monitoring script into the image
COPY collect-metrics.py .

# Expose the Prometheus metrics port
EXPOSE 8000

# Default command
CMD ["python", "collect-metrics.py"]
