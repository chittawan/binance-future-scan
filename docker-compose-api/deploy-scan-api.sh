#!/bin/bash
set -e

COMPOSE_FILE="docker-compose-scan-api.yaml"
API_IMAGE="registry.codewalk.myds.me/binance-futures-scan-api"

API_TAG="$1"
TARGET="$2"
shift 2 || true   # service names ที่เหลือ

if [ -z "$API_TAG" ]; then
  echo "❌ API_TAG is required"
  echo "Usage:"
  echo "  ./deploy-scan-api.sh <tag> all"
  echo "  ./deploy-scan-api.sh <tag> <service> [service...]"
  exit 1
fi

export API_TAG

echo "====================================="
echo "🚀 Deploy API"
echo " TAG     : $API_TAG"
echo " TARGET  : ${TARGET:-all}"
echo " COMPOSE : $COMPOSE_FILE"
echo "====================================="

# -------------------------
# Pull image ก่อน (ครั้งเดียว)
# -------------------------
echo "== Pull image =="
docker pull ${API_IMAGE}:${API_TAG}

# -------------------------
# Deploy ทั้ง compose
# -------------------------
if [ "$TARGET" = "all" ] || [ -z "$TARGET" ]; then
  echo "== Stop all services =="
  docker compose -f ${COMPOSE_FILE} stop
  sleep 10

  echo "== Remove all containers =="
  docker compose -f ${COMPOSE_FILE} rm -f
  sleep 10

  echo "== Start all services =="
  docker compose -f ${COMPOSE_FILE} up -d
  sleep 20

  echo "✅ Deploy ALL completed"
  exit 0
fi

# -------------------------
# Deploy เฉพาะ service
# -------------------------
SERVICES=("$TARGET" "$@")

for SERVICE in "${SERVICES[@]}"; do
  echo "-------------------------------------"
  echo "🔁 Deploy service: $SERVICE"

  docker compose -f ${COMPOSE_FILE} stop $SERVICE || true
  sleep 5

  docker compose -f ${COMPOSE_FILE} rm -f $SERVICE || true
  sleep 5

  API_TAG=${API_TAG} docker compose -f ${COMPOSE_FILE} up -d $SERVICE
  sleep 10
done

echo "✅ Deploy selected services completed"
