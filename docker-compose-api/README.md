# Docker Compose API - Binance Futures Bot

## 🚀 Deploy Commands

### Deploy All Services

```bash
./deploy-api.sh <API_TAG>

# ตัวอย่าง
./deploy-api.sh 2025.12.20-4
```

### Deploy เฉพาะ Film

```bash
# Pull image
docker pull registry.codewalk.myds.me/binance-futures-bot-api:<API_TAG>

# Stop และ remove container
docker compose -f docker-compose-api.yaml stop bot-film
docker compose -f docker-compose-api.yaml rm -f bot-film

# Start container
API_TAG=<API_TAG> docker compose -f docker-compose-api.yaml up -d bot-film

# ตัวอย่าง
docker compose -f docker-compose-api.yaml stop bot-film
sleep 5
docker compose -f docker-compose-api.yaml rm -f bot-film
sleep 5
API_TAG=2025.12.20-5 docker compose -f docker-compose-api.yaml up -d bot-film

# ⚠️ หมายเหตุ: คำสั่งนี้จะอัปเดตเฉพาะ bot-film เท่านั้น ไม่กระทบ services อื่น (bot01, bot02, bot03, bot-nice)
```

## 📋 Services

| Service | Port | Container Name |
|---------|------|----------------|
| bot01 | 8008 | binance-future-api-01-8008 |
| bot02 | 8010 | binance-future-api-02-8010 |
| bot03 | 8012 | binance-future-api-03-8012 |
| bot-film | 8006 | binance-future-api-film-8006 |
| bot-nice | 8004 | binance-future-api-nice-8004 |

## 📊 Resource Limits

- **Memory**: 1536m (1.5GB)
- **CPU**: 1.5 cores

# deploy ทั้ง compose
./deploy-api.sh 2025.12.20-5 all

# deploy เฉพาะ service เดียว
./deploy-api.sh 2025.12.20-5 bot-film