FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive \
    TZ=Asia/Kolkata

# Install system dependencies
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    git \
    curl \
    wget \
    unzip \
 && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements first (for Docker caching efficiency)
COPY requirements.txt /install/requirements.txt

# Install Python dependencies
RUN pip3 install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir -r /install/requirements.txt

# Copy application code
COPY ./main.zip /app/
RUN unzip main.zip && rm main.zip

# Expose Streamlit default port
EXPOSE 8508

# Run Streamlit app
ENTRYPOINT ["streamlit", "run", "src/main.py", "--server.port=8508"]
