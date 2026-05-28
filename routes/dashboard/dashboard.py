from fastapi import APIRouter
from fastapi import Query

from bson import ObjectId

from fastapi import Body

from database import (
    user_collection,
    chat_collection
)

router = APIRouter()


# Search users
@router.get("/search-users")
def search_users(
    q: str = Query("")
):

    users = []

    result = user_collection.find({

        "$or": [

            {
                "name": {
                    "$regex": q,
                    "$options": "i"
                }
            },

            {
                "email": {
                    "$regex": q,
                    "$options": "i"
                }
            }

        ]

    }).limit(10)

    for user in result:

        users.append({

            "_id": str(user["_id"]),

            "name": user["name"],

            "email": user["email"],

            "photo": user["photo"],

            "public_key": user.get("public_key")

        })

    return users


# Chat users only
@router.get("/chat-users/{my_email}")
def get_chat_users(
    my_email: str,
    page: int = 0
):

    me = user_collection.find_one({
        "email": my_email
    })

    if not me:
        return []

    my_id = me["_id"]

    chats = list(

        chat_collection.find({

            "$or": [

                {
                    "sender_id": my_id
                },

                {
                    "receiver_id": my_id
                }

            ]

        })

    )

    partner_stats = {}

    for chat in chats:
        if chat["sender_id"] == my_id and chat["receiver_id"] == my_id:
            partner_id = my_id
        else:
            partner_id = chat["receiver_id"] if chat["sender_id"] == my_id else chat["sender_id"]

        p_id_str = str(partner_id)
        msg_id = chat["_id"]
        sent_at = chat.get("sent_at")
        if not sent_at:
            sent_at = msg_id.generation_time

        # Муҳим: ТАНҲО агар фиристанда дигар корбар бошад ВА паём хонда нашуда бошад
        is_unread = (
            chat.get("sender_id") == partner_id and  # Фиристанда шарики суҳбат аст
            chat.get("receiver_id") == my_id and      # Гиранда ман ҳастам
            not chat.get("is_read", False)            # Паём хонда нашудааст
        )

        if p_id_str not in partner_stats:
            partner_stats[p_id_str] = {
                "last_message": chat,
                "last_message_time": sent_at,
                "has_unread": is_unread
            }
        else:
            if sent_at > partner_stats[p_id_str]["last_message_time"]:
                partner_stats[p_id_str]["last_message"] = chat
                partner_stats[p_id_str]["last_message_time"] = sent_at
            if is_unread:
                partner_stats[p_id_str]["has_unread"] = True

    users_with_stats = []
    for partner_id_str, stats in partner_stats.items():
        user = user_collection.find_one({
            "_id": ObjectId(partner_id_str)
        })
        if user:
            last_msg = stats["last_message"]
            last_msg_time = stats["last_message_time"]
            
            last_msg_serialized = {
                "text": last_msg["text"],
                "iv": last_msg.get("iv"),
                "sender_encrypted_key": last_msg.get("sender_encrypted_key"),
                "receiver_encrypted_key": last_msg.get("receiver_encrypted_key"),
                "sender_id": str(last_msg["sender_id"]),
                "sent_at": last_msg_time.isoformat() if hasattr(last_msg_time, "isoformat") else str(last_msg_time)
            }

            users_with_stats.append({
                "user_info": {
                    "_id": str(user["_id"]),
                    "name": user["name"],
                    "email": user["email"],
                    "photo": user["photo"],
                    "public_key": user.get("public_key")
                },
                "last_message_time": last_msg_time,
                "last_message": last_msg_serialized,
                "has_unread": stats["has_unread"]
            })

    users_with_stats.sort(
        key=lambda x: x["last_message_time"],
        reverse=True
    )

    users = []
    for item in users_with_stats:
        user_info = item["user_info"]
        user_info["has_unread"] = item["has_unread"]
        user_info["last_message"] = item["last_message"]
        users.append(user_info)

    start = page * 10
    end = start + 10

    users = users[start:end]

    return users

@router.get("/find-user/{email_name}")
def find_user(email_name: str):

    user = user_collection.find_one({
        "email": email_name
    })

    if not user:
        return {}

    return {

        "_id": str(user["_id"]),

        "name": user["name"],

        "email": user["email"],

        "photo": user["photo"],

        "public_key": user.get("public_key")

    }

@router.get("/user-by-id/{user_id}")
def get_user_by_id(user_id: str):

    user = user_collection.find_one({
        "_id": ObjectId(user_id)
    })

    if not user:
        return {}

    return {

        "_id": str(user["_id"]),

        "name": user["name"],

        "email": user["email"],

        "photo": user["photo"],

        "public_key": user.get("public_key")

    }

@router.post("/save-fcm-token")
def save_fcm_token(data = Body(...)):

    email = data.get("email")
    token = data.get("token")

    if not email or not token:
        return {
            "success": False
        }

    user_collection.update_one(

        {
            "email": email
        },

        {
            "$addToSet": {
                "fcm_tokens": token
            }
        }

    )

    return {
        "success": True
    }
