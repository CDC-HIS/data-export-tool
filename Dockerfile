FROM ubuntu:20.04

# Avoid interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Update and install dependencies
# python3-tk is needed for tkinter support
# binutils is needed for pyinstaller
# libmysqlclient-dev for mysql connector
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-tk \
    binutils \
    libgtk-3-0 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements first to leverage caching
COPY requirements.txt .

# Install Python dependencies
RUN pip3 install --upgrade pip
RUN pip3 install -r requirements.txt

# Copy project files
COPY . .

# Build with cx_Freeze for Ubuntu 20.04 compatibility
# This ensures glibc version matches the target system
CMD ["python3", "setup.py", "build"]
