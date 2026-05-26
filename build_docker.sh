#!/bin/bash

# Clean previous build
rm -rf export_distributable

# Build the Docker image
echo "Building Docker image..."
docker build -t export-tool-builder .

# Run the container to build (no volume mounts needed)
echo "Running build inside Docker container..."
CONTAINER_ID=$(docker create export-tool-builder)
docker start -a $CONTAINER_ID

# Copy build output from container
echo "Copying build output..."
docker cp $CONTAINER_ID:/app/export_distributable ./export_distributable

# Clean up container
docker rm $CONTAINER_ID > /dev/null 2>&1

# Fix permissions (Docker runs as root by default)
echo "Fixing file permissions..."
sudo chown -R $USER:$USER export_distributable/ 2>/dev/null

echo "Build complete. Output is in 'export_distributable/' directory."
echo "Run the executable with: ./export_distributable/bin/export"
