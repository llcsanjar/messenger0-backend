import os
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

from routes.registration.registration import router as registration_router
from routes.dashboard.dashboard import router as dashboard_router
from routes.dashboard.chat import router as chat_router

load_dotenv()

app = FastAPI()

# Restrict CORS to only allowed origin loaded from .env
ALLOWED_ORIGIN = os.getenv("ALLOWED_ORIGIN", "http://localhost:5173")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[ALLOWED_ORIGIN],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Custom HTTP Middleware to return 403 Forbidden for non-allowed HTTP requests (like Postman or raw curls)
@app.middleware("http")
async def origin_restriction_middleware(request: Request, call_next):
    # Always allow preflight OPTIONS requests
    if request.method == "OPTIONS":
        return await call_next(request)
        
    # Allow welcome endpoint
    if request.url.path == "/":
        return await call_next(request)
        
    origin = request.headers.get("origin")
    referer = request.headers.get("referer")
    
    allowed_origin_norm = ALLOWED_ORIGIN.rstrip('/')
    
    # 1. If Origin header is present, validate it
    if origin:
        if origin.rstrip('/') != allowed_origin_norm:
            return Response("Forbidden: Origin not allowed", status_code=403)
            
    # 2. If Referer is present, validate it
    elif referer:
        if not referer.startswith(allowed_origin_norm):
            return Response("Forbidden: Referer not allowed", status_code=403)
            
    # 3. If both Origin and Referer are missing, it's a direct API call (like Postman or python requests)
    else:
        return Response("Forbidden: Direct API access is prohibited", status_code=403)
        
    return await call_next(request)

app.include_router(registration_router)
app.include_router(dashboard_router)
app.include_router(chat_router)

@app.get("/")
def root():
    return {
        "message": "Backend running"
    }

