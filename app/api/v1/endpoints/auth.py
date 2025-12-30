from app.services.auth_service import require_auth, create_access_token
from fastapi import APIRouter, HTTPException, Depends
from app.models.login_request import LoginRequest

router = APIRouter()

# ================= API =================


@router.post("/login")
async def login(request: LoginRequest):
    # demo auth (ของจริงควรต่อ DB / LDAP)
    if request.username == "admin" and request.password == "admin":
        token = create_access_token(request.username)
        return {
            "access_token": token,
            "token_type": "bearer"
        }
    raise HTTPException(status_code=401, detail="Invalid credentials")


@router.get("/me")
async def get_current_user(user=Depends(require_auth)):
    return {
        "username": user["sub"]
    }
