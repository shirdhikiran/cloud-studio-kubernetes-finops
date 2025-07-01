FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first (for better caching)
COPY requirements/prod.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ src/
COPY config/ config/
COPY scripts/ scripts/
COPY pyproject.toml .

# Install the package
RUN pip install -e .

# Create non-root user
RUN useradd --create-home --shell /bin/bash finops
RUN chown -R finops:finops /app
USER finops

# Create directories for data and logs
RUN mkdir -p /app/data /app/logs

# Set Python path
ENV PYTHONPATH=/app/src

# Default command
CMD ["python", "scripts/discovery_runner.py"]