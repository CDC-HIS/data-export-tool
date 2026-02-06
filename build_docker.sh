#!/bin/bash

# Build the Docker image
echo "Building Docker image..."
docker build -t export-tool-builder .

# Create dist directory if it doesn't exist
mkdir -p dist

# Run the container to build the executable
# We mount the local 'dist' directory to /app/dist inside the container
# so the generated executable survives after container exit.
echo "Running build inside Docker container..."
docker run --rm -v "$(pwd)/dist:/app/dist" export-tool-builder

# Fix permissions (Docker runs as root by default)
echo "Fixing file permissions..."
sudo chown -R $USER:$USER dist/

echo "Build complete. Check 'dist/ExportTool'."
echo "To verify compatibility, run: file dist/ExportTool"
