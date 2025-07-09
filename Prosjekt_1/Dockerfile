# Use a lightweight Python image
FROM python:3.11-slim

# Avoid Python writing .pyc files and buffering stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install system dependencies (for GUI and ODBC)
RUN apt-get update && apt-get install -y \
    tk \
    unixodbc-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy everything into the image
COPY . /app/

# Install pip dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Set default run script (can be changed to pipelineGUI.py if needed)
CMD ["python", "runPipeline.py"]
