FROM ubuntu:20.04

# Avoid interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Update and install dependencies
# python3-tk is needed for tkinter support in the build
# binutils is needed for pyinstaller
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-tk \
    binutils \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements first to leverage caching
COPY requirements.txt .

# Install Python dependencies
# Update pip first
RUN pip3 install --upgrade pip
RUN pip3 install -r requirements.txt

# Copy project files
COPY . .

# Build command (default)
CMD ["pyinstaller", "--noconfirm", "--clean", "--onefile", "--windowed", "--name", "ExportTool", "--icon", "moh.ico", "--add-data", "sql_queries:sql_queries", "--add-data", "export_config.json:.", "--add-data", "moh.ico:.", "--add-data", "moh.png:.", "export.py"]
