from datetime import datetime, timedelta
from fastapi import HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import jwt
from jose.exceptions import JWTError, ExpiredSignatureError
import os

# ================= JWT CONFIG =================
JWT_ALGORITHM = "HS256"
JWT_EXPIRATION_MINUTES = 1440 * 7  # 7 days
JWT_ISSUER = "https://auth.codewalk.myds.me"
JWT_AUDIENCE = "all-services"

JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "your-secret-key")

security = HTTPBearer()

# ================= AUTH FUNCTIONS =================


def authenticate_user(username: str, password: str) -> bool:
    # demo auth (ของจริงควรต่อ DB / LDAP)
    if username == "admin" and password == "admin":
        return True
    return False


def create_access_token(username: str) -> str:
    payload = {
        "sub": username,
        "iss": JWT_ISSUER,
        "aud": JWT_AUDIENCE,
        "iat": datetime.utcnow(),
        "exp": datetime.utcnow() + timedelta(minutes=JWT_EXPIRATION_MINUTES),
    }

    token = jwt.encode(
        payload,
        JWT_SECRET_KEY,
        algorithm=JWT_ALGORITHM
    )
    return token


def verify_jwt(token: str):
    try:
        payload = jwt.decode(
            token,
            JWT_SECRET_KEY,
            algorithms=[JWT_ALGORITHM],
            issuer=JWT_ISSUER,
            audience=JWT_AUDIENCE
        )
        return payload
    except ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")


def require_auth(
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    payload = verify_jwt(credentials.credentials)

    if "sub" not in payload:
        raise HTTPException(status_code=401, detail="Invalid token payload")

    return payload
