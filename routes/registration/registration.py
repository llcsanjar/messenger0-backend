from fastapi import APIRouter
from pydantic import BaseModel

from database import user_collection

router = APIRouter()


class RegisterUser(BaseModel):
    name: str
    email: str
    photo: str
    public_key: str = None


@router.post("/register")
def register_user(user: RegisterUser):

    existing_user = user_collection.find_one({
        "email": user.email
    })

    if existing_user:
        if user.public_key:
            user_collection.update_one(
                {"_id": existing_user["_id"]},
                {"$set": {"public_key": user.public_key}}
            )
        return {
            "message": "User already exists",
            "user_id": str(existing_user["_id"])
        }

    result = user_collection.insert_one({
        "name": user.name,
        "email": user.email,
        "photo": user.photo,
        "public_key": user.public_key,
    })

    return {
        "message": "User registered successfully",
        "user_id": str(result.inserted_id)
    }

