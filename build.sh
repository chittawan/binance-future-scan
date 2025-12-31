#!/bin/bash

# Build and push Docker image for binance-futures-bot-api
# This script automatically generates a tag based on the current date

DATE_TAG=$(date +"%Y.%m.%d")
IMAGE="registry.codewalk.myds.me/binance-futures-scan-api"

# Get the last build number for today
LAST_NUMBER=$(crane ls $IMAGE 2>/dev/null \
  | grep "^${DATE_TAG}-" \
  | sed "s/${DATE_TAG}-//" \
  | sort -n \
  | tail -1)

LAST_NUMBER=${LAST_NUMBER:-0}
RUNNING_NUMBER=$((LAST_NUMBER + 1))
TAG="${DATE_TAG}-${RUNNING_NUMBER}"

echo "=========================================="
echo "Building Docker Image"
echo "=========================================="
echo "Image: $IMAGE"
echo "Tag: $TAG"
echo "Platforms: linux/amd64,linux/arm64"
echo "=========================================="
echo ""

# Build and push
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -t registry.codewalk.myds.me/binance-futures-scan-api:$TAG \
  --push .

if [ $? -eq 0 ]; then
  echo ""
  echo "=========================================="
  echo "✓ Build successful!"
  echo "=========================================="
  echo "New image tag: $TAG"
  echo ""
  echo "To update docker-compose files, change the image tag to:"
  echo "  image: registry.codewalk.myds.me/binance-futures-scan-api:$TAG"
  echo "=========================================="
else
  echo ""
  echo "=========================================="
  echo "✗ Build failed!"
  echo "=========================================="
  exit 1
fi


