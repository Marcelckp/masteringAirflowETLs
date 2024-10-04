FROM apache/airflow:latest

# Install system dependencies
USER root
RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    wget \
    && apt-get clean

# Install Node.js
RUN curl -sL https://deb.nodesource.com/setup_14.x | bash -
RUN apt-get install -y nodejs

# Install Playwright and its dependencies
RUN npm install -g playwright
RUN npx playwright install-deps
RUN npx playwright install

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright for Python
RUN pip install --no-cache-dir playwright
RUN playwright install

# Create necessary directories
RUN mkdir -p /opt/airflow/data_lake/raw /opt/airflow/data_lake/refined /opt/airflow/data_collection

USER airflow