FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ ./src/

# Set Python path
ENV PYTHONPATH=/app

# Expose API port
EXPOSE 8000

# Default command (will be overridden in docker-compose if needed)
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]
