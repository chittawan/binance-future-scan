python3 --version
# Python 3.11
brew install python@3.12
brew link python@3.12 --force
python3.12 -m venv venv312

source venv312/bin/activate

pip install --upgrade pip
pip install fastapi 'uvicorn[standard]' dotenv
pip install binance-sdk-derivatives-trading-usds-futures


---

which python3
which pip3
which uvicowrn

pip3 show fastapi
pip3 show httpx
pip3 show python-dotenv
pip3 show binance-sdk-derivatives-trading-usds-futures

# run app
source venv312/bin/activate
uvicorn app.main:app --reload

# build and run docker file
pip freeze > requirements.txt

# Build Docker image
docker build -t binance-futures-bot:latest .

# docker push to registry : https://registry.codewalk.myds.me/

# Tag the image for your registry (replace `your-namespace`)
### TAG=$(date +"%Y.%m.%d")-02
### IMAGE=binance-futures-bot
### REGISTRY=registry.codewalk.myds.me
### docker build -t $IMAGE:$TAG .
### 
### docker tag $IMAGE:$TAG $REGISTRY/$IMAGE:$TAG
### docker push $REGISTRY/$IMAGE:$TAG

# initial docker BuildX
docker buildx create --use --name multi-builder
docker buildx inspect --bootstrap

# COMMAND ADD IMAGE
DATE_TAG=$(date +"%Y.%m.%d")
IMAGE="registry.codewalk.myds.me/binance-futures-bot-api"

LAST_NUMBER=$(crane ls $IMAGE \
  | grep "^${DATE_TAG}-" \
  | sed "s/${DATE_TAG}-//" \
  | sort -n \
  | tail -1)

LAST_NUMBER=${LAST_NUMBER:-0}
RUNNING_NUMBER=$((LAST_NUMBER + 1))
TAG="${DATE_TAG}-${RUNNING_NUMBER}"

echo "Using TAG: $TAG"

docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -t registry.codewalk.myds.me/binance-futures-bot-api:$TAG \
  --push .


# Test
./venv312/bin/python3 -m pytest tests/unit/endpoints/
./venv312/bin/python3 -m pytest tests/unit/services/

./venv312/bin/python3 -m pytest --cov=app

pytest tests/unit/services/test_kline_websocket_service.py::test_get_stream_health_stale_1_5_hours -v

# Run complete recovery flow test
pytest tests/unit/services/test_kline_websocket_service.py::test_complete_recovery_flow_1_5_hours -v

# Run with coverage
pytest tests/unit/services/test_kline_websocket_service.py --cov=app.services.kline_websocket_service -v

# Run the boundary test
pytest tests/unit/services/test_kline_websocket_service.py::test_1h_interval_boundary_update_13_59_to_14_00 -v

# Run the multiple updates test
pytest tests/unit/services/test_kline_websocket_service.py::test_1h_interval_multiple_updates_during_hour -v

# Run both
pytest tests/unit/services/test_kline_websocket_service.py::test_1h_interval_boundary_update_13_59_to_14_00 tests/unit/services/test_kline_websocket_service.py::test_1h_interval_multiple_updates_during_hour -v