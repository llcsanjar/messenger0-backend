from fastapi import APIRouter
from fastapi import WebSocket
from fastapi import WebSocketDisconnect
from fastapi import Query

from bson import ObjectId

from database import (
    chat_collection,
    user_collection,
    user_status_collection
)

import json
from datetime import datetime, timezone

from routes.dashboard.firebase_push import send_push_notification

import asyncio

router = APIRouter()

# Барои пайгирии пайвастҳои фаъоли ҳар корбар
active_connections = {}  # {user_email: count}
clients = {}  # {websocket: {"connected": True, "user_email": email, "user_id": user_id}}
active_ringing_calls = {}  # {receiver_id_str: { "sender_id": ..., "sender_email": ..., "sender_name": ..., "sender_photo": ..., "call_type": ..., "timestamp": ... }}

CALL_NOTIF_TRANSLATIONS = {
    "tg": "{name} ба шумо занг зада истодааст...",
    "ru": "{name} звонит вам...",
    "en": "{name} is calling you...",
    "fa": "{name} به شما زنگ می‌زند..."
}

CHAT_NOTIF_TRANSLATIONS = {
    "tg": "Паёми нав аз тарафи {name}",
    "ru": "Новое сообщение от {name}",
    "en": "New message from {name}",
    "fa": "پیام جدید از طرف {name}"
}

async def periodic_call_notification(receiver_id_str, caller_name):
    # Ringing timeout is 30 seconds, so we run at most 15 times (15 * 2 = 30s)
    for _ in range(15):
        await asyncio.sleep(2.0)
        # Check if the call is still ringing
        if receiver_id_str not in active_ringing_calls:
            break
            
        # Get the receiver's fresh data from the database
        try:
            receiver_obj = user_collection.find_one({"_id": ObjectId(receiver_id_str)})
            if not receiver_obj:
                break
                
            receiver_email_val = receiver_obj.get("email")
            is_online = False
            if receiver_email_val:
                if receiver_email_val in active_connections and active_connections[receiver_email_val] > 0:
                    is_online = True
                else:
                    status_doc = user_status_collection.find_one({"email": receiver_email_val})
                    if status_doc and status_doc.get("is_online", False):
                        is_online = True
                        
            # Агар гиранда онлайн бошад, огоҳинома равон накун!
            if is_online:
                continue
                
            tokens = receiver_obj.get("fcm_tokens", [])
            if not tokens:
                continue
                
            # Determine the language
            lang = receiver_obj.get("language", "ru")
            template = CALL_NOTIF_TRANSLATIONS.get(lang, CALL_NOTIF_TRANSLATIONS["ru"])
            body_text = template.replace("{name}", caller_name)
            
            # Send notification to all tokens
            async def send_single_token(token):
                try:
                    await asyncio.to_thread(
                        send_push_notification, 
                        token, 
                        caller_name,  # Pass caller's name as the title!
                        body_text, 
                        "call"
                    )
                except Exception as e:
                    print(f"Error sending periodic call push: {e}")
                    
            tasks = [send_single_token(t) for t in tokens]
            if tasks:
                await asyncio.gather(*tasks)
        except Exception as ex:
            print(f"Error in periodic_call_notification: {ex}")
            break

# Функсияи бехатар барои фиристодани паём ба ҳама клиентҳо
async def safe_broadcast(message):
    client_list = list(clients.keys())
    disconnected_clients = []
    
    for client in client_list:
        if client not in clients:
            continue
            
        status = clients[client]
        if not status.get("connected", False):
            disconnected_clients.append(client)
            continue
            
        try:
            await client.send_text(message)
        except Exception as e:
            print(f"Error sending to client: {e}")
            disconnected_clients.append(client)
    
    for client in disconnected_clients:
        if client in clients:
            try:
                del clients[client]
            except Exception as e:
                print(f"Error deleting client: {e}")

def get_user_status_from_db(user_email):
    try:
        status_doc = user_status_collection.find_one({"email": user_email})
        if status_doc:
            return {
                "is_online": status_doc.get("is_online", False),
                "last_online": status_doc.get("last_online")
            }
    except Exception as e:
        print(f"Error getting user status from DB: {e}")
    return {
        "is_online": False,
        "last_online": None
    }

def update_user_status_in_db(user_email, is_online):
    try:
        now = datetime.now(timezone.utc)
        user_status_collection.update_one(
            {"email": user_email},
            {"$set": {
                "is_online": is_online,
                "last_online": now if not is_online else None,
                "updated_at": now
            }},
            upsert=True
        )
    except Exception as e:
        print(f"Error updating user status in DB: {e}")

async def broadcast_user_status(user_email, user_id, is_online):
    last_online = None
    if not is_online:
        status = get_user_status_from_db(user_email)
        last_online = status.get("last_online")
        if last_online and last_online.tzinfo is None:
            last_online = last_online.replace(tzinfo=timezone.utc)

    response = {
        "type": "user_status",
        "user_email": user_email,
        "user_id": user_id,
        "is_online": is_online,
        "last_online": last_online.isoformat() if last_online and hasattr(last_online, "isoformat") else None
    }
    await safe_broadcast(json.dumps(response))

# API барои гирифтани статуси ҳамаи корбарон
@router.get("/user-statuses")
def get_all_user_statuses():
    statuses = {}
    try:
        for doc in user_status_collection.find({}):
            email = doc.get("email")
            if email:
                last_online = doc.get("last_online")
                if last_online and last_online.tzinfo is None:
                    last_online = last_online.replace(tzinfo=timezone.utc)

                statuses[email] = {
                    "is_online": doc.get("is_online", False),
                    "last_online": last_online.isoformat() if last_online and hasattr(last_online, "isoformat") else None
                }
    except Exception as e:
        print(f"Error getting user statuses: {e}")
    return statuses

@router.get("/user-status/{email}")
def get_user_status(email: str):
    status = get_user_status_from_db(email)
    return {
        "email": email,
        "is_online": status["is_online"],
        "last_online": status["last_online"].isoformat() if status["last_online"] and hasattr(status["last_online"], "isoformat") else None
    }

# Load old messages with pagination
@router.get("/messages/{my_email}/{receiver_id}")
def get_messages(
    my_email: str,
    receiver_id: str,
    limit: int = Query(default=10, ge=1, le=50),
    before: str = Query(default=None, description="ISO format date to get messages before this date")
):
    me = user_collection.find_one({"email": my_email})
    if not me:
        return []
    my_id = me["_id"]
    my_email_str = my_email
    receiver_object_id = ObjectId(receiver_id)
    query = {
        "$or": [
            {"sender_id": my_id, "receiver_id": receiver_object_id},
            {"sender_id": receiver_object_id, "receiver_id": my_id}
        ]
    }
    if before:
        try:
            before_date = datetime.fromisoformat(before)
            if before_date.tzinfo is None:
                before_date = before_date.replace(tzinfo=timezone.utc)
            query["sent_at"] = {"$lt": before_date}
        except Exception as e:
            print(f"Error parsing before date: {e}")
    messages = list(chat_collection.find(query).sort("sent_at", -1).limit(limit))
    messages.reverse()
    result = []
    for msg in messages:
        sent_at = msg.get("sent_at")
        if sent_at and sent_at.tzinfo is None:
            sent_at = sent_at.replace(tzinfo=timezone.utc)
        read_at = msg.get("read_at")
        if read_at and read_at.tzinfo is None:
            read_at = read_at.replace(tzinfo=timezone.utc)
        deleted_for_users = msg.get("deleted_for_users", [])
        is_deleted_for_me = my_email_str in deleted_for_users
        is_deleted_for_everyone = msg.get("is_deleted_for_everyone", False)
        reply_to_message = None
        if msg.get("reply_to") and not is_deleted_for_everyone:
            try:
                reply_msg = chat_collection.find_one({"_id": ObjectId(msg["reply_to"])})
                if reply_msg:
                    reply_deleted_for_everyone = reply_msg.get("is_deleted_for_everyone", False)
                    reply_sender = user_collection.find_one({"_id": reply_msg["sender_id"]})
                    if reply_deleted_for_everyone:
                        reply_to_message = {
                            "_id": str(reply_msg["_id"]),
                            "sender_id": str(reply_msg["sender_id"]),
                            "sender_name": reply_sender.get("name", "") if reply_sender else "",
                            "text": "deleted_message",
                            "iv": None,
                            "sender_encrypted_key": None,
                            "receiver_encrypted_key": None,
                            "duration": 0,
                            "is_deleted_for_everyone": True
                        }
                    else:
                        reply_to_message = {
                            "_id": str(reply_msg["_id"]),
                            "sender_id": str(reply_msg["sender_id"]),
                            "sender_name": reply_sender.get("name", "") if reply_sender else "",
                            "text": reply_msg["text"],
                            "iv": reply_msg.get("iv"),
                            "sender_encrypted_key": reply_msg.get("sender_encrypted_key"),
                            "receiver_encrypted_key": reply_msg.get("receiver_encrypted_key"),
                            "duration": reply_msg.get("duration", 0),
                            "is_deleted_for_everyone": False
                        }
            except:
                pass
        reactions = msg.get("reactions", {}) if not is_deleted_for_everyone else {}
        text = msg["text"]
        message_type = msg.get("message_type", "text")
        if is_deleted_for_everyone:
            text = "deleted_message"
            message_type = "deleted"
        result.append({
            "_id": str(msg["_id"]),
            "sender_id": str(msg["sender_id"]),
            "receiver_id": str(msg["receiver_id"]),
            "text": text,
            "message_type": message_type,
            "iv": msg.get("iv") if not is_deleted_for_everyone else None,
            "sender_encrypted_key": msg.get("sender_encrypted_key") if not is_deleted_for_everyone else None,
            "receiver_encrypted_key": msg.get("receiver_encrypted_key") if not is_deleted_for_everyone else None,
            "is_read": msg.get("is_read", False),
            "sent_at": sent_at.isoformat(),
            "read_at": read_at.isoformat() if hasattr(read_at, "isoformat") and read_at else None,
            "reply_to": reply_to_message,
            "reactions": reactions,
            "duration": msg.get("duration", 0) if not is_deleted_for_everyone else 0,
            "is_deleted_for_me": is_deleted_for_me,
            "is_deleted_for_everyone": is_deleted_for_everyone,
            "deleted_for_users": deleted_for_users,
            "is_edited": msg.get("is_edited", False),
            "call_type": msg.get("call_type"),
            "call_status": msg.get("call_status"),
            "call_duration": msg.get("call_duration", 0)
        })
    return result

async def delayed_notification(message_id, receiver, sender):
    await asyncio.sleep(1)
    msg = chat_collection.find_one({"_id": ObjectId(message_id)})
    if not msg or msg.get("is_read"):
        return
        
    # Агар гиранда онлайн бошад, огоҳинома равон накун!
    receiver_email = receiver.get("email")
    if receiver_email:
        if receiver_email in active_connections and active_connections[receiver_email] > 0:
            return
        status_doc = user_status_collection.find_one({"email": receiver_email})
        if status_doc and status_doc.get("is_online", False):
            return
            
    tokens = receiver.get("fcm_tokens", [])
    
    # Муайян кардани забони огоҳиномаи паём дар бекенд
    lang = receiver.get("language", "ru")
    template = CHAT_NOTIF_TRANSLATIONS.get(lang, CHAT_NOTIF_TRANSLATIONS["ru"])
    body_text = template.replace("{name}", sender["name"])
    
    async def send_notification_thread(token):
        try:
            await asyncio.to_thread(
                send_push_notification, 
                token, 
                sender["name"],  # Pass sender's name as the title!
                body_text, 
                "chat"
            )
        except Exception as e:
            print(f"Notification error: {e}")
    tasks = [send_notification_thread(token) for token in tokens]
    if tasks:
        await asyncio.gather(*tasks)

# Helper to relay message to a user based on their ID or email (useful for multiple active tabs)
async def relay_message_to_user(receiver_id_str, receiver_email, payload):
    message_str = json.dumps(payload)
    sent = False
    for ws, info in list(clients.items()):
        if info.get("connected", False):
            # match by either user_id or user_email
            if (receiver_id_str and info.get("user_id") == str(receiver_id_str)) or (receiver_email and info.get("user_email") == receiver_email):
                try:
                    await ws.send_text(message_str)
                    sent = True
                except Exception as e:
                    print(f"Error relaying message to {receiver_email or receiver_id_str}: {e}")
    return sent

# WebSocket Chat
@router.websocket("/ws/chat")
async def websocket_chat(websocket: WebSocket):
    import os
    origin = websocket.headers.get("origin")
    allowed_origin = os.getenv("ALLOWED_ORIGIN", "http://localhost:5173").rstrip('/')
    if not origin or origin.rstrip('/') != allowed_origin:
        await websocket.accept()
        await websocket.close(code=4003)
        return
        
    await websocket.accept()
    user_email = None
    user_id = None
    
    try:
        try:
            data = await asyncio.wait_for(websocket.receive_text(), timeout=10.0)
            first_message = json.loads(data)
            if first_message.get("type") == "auth":
                user_email = first_message.get("email")
                source = first_message.get("source", "unknown")
                if user_email:
                    user_obj = user_collection.find_one({"email": user_email})
                    if user_obj:
                        user_id = str(user_obj["_id"])
                        
                        # Захираи забони корбар
                        client_lang = first_message.get("language")
                        if client_lang:
                            user_collection.update_one(
                                {"_id": user_obj["_id"]},
                                {"$set": {"language": client_lang}}
                            )
                        
                        # Check for active ringing call for this user
                        if str(user_id) in active_ringing_calls:
                            pending_call = active_ringing_calls[str(user_id)]
                            now = datetime.now(timezone.utc)
                            call_time = pending_call["timestamp"]
                            if (now - call_time).total_seconds() < 35:
                                payload = {
                                    "type": "incoming_call",
                                    "sender_id": pending_call["sender_id"],
                                    "sender_email": pending_call["sender_email"],
                                    "sender_name": pending_call["sender_name"],
                                    "sender_photo": pending_call["sender_photo"],
                                    "call_type": pending_call["call_type"]
                                }
                                await websocket.send_text(json.dumps(payload))
                            else:
                                active_ringing_calls.pop(str(user_id), None)
                        
                        # Шумораи пайвастҳои фаъоли ин корбарро зиёд кунед
                        if user_email not in active_connections:
                            active_connections[user_email] = 0
                        active_connections[user_email] += 1
                        
                        # Танҳо агар ин аввалин пайваст бошад, статусро ба онлайн иваз кунед
                        if active_connections[user_email] == 1:
                            update_user_status_in_db(user_email, True)
                            await broadcast_user_status(user_email, user_id, True)
                        print(f"User {user_email} connected. Active connections: {active_connections[user_email]} (source: {source})")
        except asyncio.TimeoutError:
            pass
        except WebSocketDisconnect:
            return
        except Exception as e:
            print(f"Error on initial message: {e}")
        
        clients[websocket] = {
            "connected": True,
            "user_email": user_email,
            "user_id": user_id
        }
        
        while True:
            try:
                data = await asyncio.wait_for(websocket.receive_text(), timeout=300.0)
            except asyncio.TimeoutError:
                if websocket in clients and clients[websocket].get("connected"):
                    continue
                else:
                    break
            except WebSocketDisconnect:
                break
            
            message = json.loads(data)
            
            # Dashboard close message
            if message.get("type") == "dashboard_close":
                if user_email and user_email in active_connections:
                    # Ин пайвастро аз ҳисоб бардоред
                    pass
                continue

            # --- WebRTC Calling Events ---
            msg_type = message.get("type")

            if msg_type == "call_initiate":
                call_type = message.get("call_type", "voice")
                sender_email = message.get("sender_email")
                receiver_id = message.get("receiver_id")
                sender_obj = user_collection.find_one({"email": sender_email})
                if sender_obj:
                    # Сабти занги фаъол барои офлайн-ба-онлайн
                    active_ringing_calls[str(receiver_id)] = {
                        "sender_id": str(sender_obj["_id"]),
                        "sender_email": sender_email,
                        "sender_name": sender_obj.get("name", ""),
                        "sender_photo": sender_obj.get("photo", ""),
                        "call_type": call_type,
                        "timestamp": datetime.now(timezone.utc)
                    }
                    payload = {
                        "type": "incoming_call",
                        "sender_id": str(sender_obj["_id"]),
                        "sender_email": sender_email,
                        "sender_name": sender_obj.get("name", ""),
                        "sender_photo": sender_obj.get("photo", ""),
                        "call_type": call_type
                    }
                    await relay_message_to_user(receiver_id, None, payload)
                    
                    # Фиристодани огоҳиномаи фаврии Firebase оғоз мегардад
                    try:
                        receiver_obj = user_collection.find_one({"_id": ObjectId(receiver_id)})
                        if receiver_obj:
                            receiver_email_val = receiver_obj.get("email")
                            is_online = False
                            if receiver_email_val:
                                if receiver_email_val in active_connections and active_connections[receiver_email_val] > 0:
                                    is_online = True
                                else:
                                    status_doc = user_status_collection.find_one({"email": receiver_email_val})
                                    if status_doc and status_doc.get("is_online", False):
                                        is_online = True
                                        
                            # Агар гиранда онлайн бошад, огоҳинома равон накун!
                            if not is_online:
                                tokens = receiver_obj.get("fcm_tokens", [])
                                if tokens:
                                    lang = receiver_obj.get("language", "ru")
                                    template = CALL_NOTIF_TRANSLATIONS.get(lang, CALL_NOTIF_TRANSLATIONS["ru"])
                                    body_text = template.replace("{name}", sender_obj.get("name", "Касе"))
                                    
                                    async def send_first_notif(tkn):
                                        try:
                                            await asyncio.to_thread(
                                                send_push_notification, 
                                                tkn, 
                                                sender_obj.get("name", "Касе"),  # Pass caller's name as the title!
                                                body_text, 
                                                "call"
                                            )
                                        except Exception as err:
                                            print(f"Error sending first push call: {err}")
                                            
                                    first_tasks = [send_first_notif(t) for t in tokens]
                                    if first_tasks:
                                        asyncio.create_task(asyncio.gather(*first_tasks))
                    except Exception as e_init:
                        print(f"Error in first push notification call init: {e_init}")
                        
                    # Оғози заминавии фиристодани даврӣ ҳар 2 сония
                    asyncio.create_task(periodic_call_notification(str(receiver_id), sender_obj.get("name", "Касе")))
                continue

            elif msg_type == "call_accept":
                receiver_id = message.get("receiver_id")
                sender_email = message.get("sender_email")
                if user_id:
                    active_ringing_calls.pop(str(user_id), None)
                sender_obj = user_collection.find_one({"email": sender_email})
                if sender_obj:
                    payload = {
                        "type": "call_accepted",
                        "sender_id": str(sender_obj["_id"]),
                        "sender_email": sender_email
                    }
                    await relay_message_to_user(receiver_id, None, payload)
                continue

            elif msg_type == "call_reject":
                receiver_id = message.get("receiver_id")
                sender_email = message.get("sender_email")
                if user_id:
                    active_ringing_calls.pop(str(user_id), None)
                sender_obj = user_collection.find_one({"email": sender_email})
                if sender_obj:
                    payload = {
                        "type": "call_rejected",
                        "sender_id": str(sender_obj["_id"]),
                        "sender_email": sender_email
                    }
                    await relay_message_to_user(receiver_id, None, payload)
                continue

            elif msg_type == "call_end":
                receiver_id = message.get("receiver_id")
                sender_email = message.get("sender_email")
                call_type = message.get("call_type", "voice")
                call_status = message.get("call_status", "completed")
                call_duration = int(message.get("call_duration", 0))

                active_ringing_calls.pop(str(receiver_id), None)
                if user_id:
                    active_ringing_calls.pop(str(user_id), None)

                sender_obj = user_collection.find_one({"email": sender_email})
                if sender_obj:
                    sender_id = sender_obj["_id"]
                    rec_obj_id = ObjectId(receiver_id)
                    receiver_obj = user_collection.find_one({"_id": rec_obj_id})

                    payload = {
                        "type": "call_ended",
                        "sender_id": str(sender_id),
                        "sender_email": sender_email
                    }
                    await relay_message_to_user(receiver_id, None, payload)

                    if message.get("save_log"):
                        caller_email = message.get("caller_email", sender_email)
                        receiver_email_val = message.get("receiver_email")

                        caller_obj = user_collection.find_one({"email": caller_email})
                        if caller_obj:
                            c_id = caller_obj["_id"]
                            r_id = rec_obj_id
                            if receiver_email_val:
                                r_obj = user_collection.find_one({"email": receiver_email_val})
                                if r_obj:
                                    r_id = r_obj["_id"]

                            chat_data = {
                                "sender_id": c_id,
                                "receiver_id": r_id,
                                "text": f"call_{call_type}_{call_status}",
                                "message_type": "call",
                                "call_type": call_type,
                                "call_status": call_status,
                                "call_duration": call_duration,
                                "iv": None,
                                "sender_encrypted_key": None,
                                "receiver_encrypted_key": None,
                                "is_read": False,
                                "sent_at": datetime.now(timezone.utc),
                                "read_at": None,
                                "reply_to": None,
                                "reactions": {},
                                "is_deleted_for_everyone": False,
                                "deleted_for_users": []
                            }
                            result = chat_collection.insert_one(chat_data)
                            message_id = str(result.inserted_id)

                            response = {
                                "_id": message_id,
                                "sender_id": str(c_id),
                                "receiver_id": str(r_id),
                                "receiver_email": r_obj["email"] if (receiver_email_val and r_obj) else receiver_email_val,
                                "text": chat_data["text"],
                                "message_type": "call",
                                "call_type": call_type,
                                "call_status": call_status,
                                "call_duration": call_duration,
                                "is_read": False,
                                "sent_at": chat_data["sent_at"].isoformat(),
                                "reply_to": None,
                                "reactions": {},
                                "is_deleted_for_me": False,
                                "is_deleted_for_everyone": False,
                                "is_edited": False
                            }
                            await safe_broadcast(json.dumps(response))
                continue

            elif msg_type == "webrtc_offer":
                receiver_id = message.get("receiver_id")
                sender_email = message.get("sender_email")
                sender_obj = user_collection.find_one({"email": sender_email})
                if sender_obj:
                    payload = {
                        "type": "webrtc_offer",
                        "sender_id": str(sender_obj["_id"]),
                        "sender_email": sender_email,
                        "sdp": message.get("sdp")
                    }
                    await relay_message_to_user(receiver_id, None, payload)
                continue

            elif msg_type == "webrtc_answer":
                receiver_id = message.get("receiver_id")
                sender_email = message.get("sender_email")
                sender_obj = user_collection.find_one({"email": sender_email})
                if sender_obj:
                    payload = {
                        "type": "webrtc_answer",
                        "sender_id": str(sender_obj["_id"]),
                        "sender_email": sender_email,
                        "sdp": message.get("sdp")
                    }
                    await relay_message_to_user(receiver_id, None, payload)
                continue

            elif msg_type == "ice_candidate":
                receiver_id = message.get("receiver_id")
                sender_email = message.get("sender_email")
                sender_obj = user_collection.find_one({"email": sender_email})
                if sender_obj:
                    payload = {
                        "type": "ice_candidate",
                        "sender_id": str(sender_obj["_id"]),
                        "sender_email": sender_email,
                        "candidate": message.get("candidate")
                    }
                    await relay_message_to_user(receiver_id, None, payload)
                continue

            elif msg_type == "camera_toggle":
                receiver_id = message.get("receiver_id")
                sender_email = message.get("sender_email")
                sender_obj = user_collection.find_one({"email": sender_email})
                if sender_obj:
                    payload = {
                        "type": "camera_toggle",
                        "sender_id": str(sender_obj["_id"]),
                        "sender_email": sender_email,
                        "enabled": message.get("enabled", True)
                    }
                    await relay_message_to_user(receiver_id, None, payload)
                continue

            if message.get("type") == "auth":
                user_email = message.get("email")
                source = message.get("source", "unknown")
                if user_email:
                    user_obj = user_collection.find_one({"email": user_email})
                    if user_obj:
                        user_id = str(user_obj["_id"])
                        
                        # Захираи забони корбар
                        client_lang = message.get("language")
                        if client_lang:
                            user_collection.update_one(
                                {"_id": user_obj["_id"]},
                                {"$set": {"language": client_lang}}
                            )
                        
                        # Check for active ringing call for this user
                        if str(user_id) in active_ringing_calls:
                            pending_call = active_ringing_calls[str(user_id)]
                            now = datetime.now(timezone.utc)
                            call_time = pending_call["timestamp"]
                            if (now - call_time).total_seconds() < 35:
                                payload = {
                                    "type": "incoming_call",
                                    "sender_id": pending_call["sender_id"],
                                    "sender_email": pending_call["sender_email"],
                                    "sender_name": pending_call["sender_name"],
                                    "sender_photo": pending_call["sender_photo"],
                                    "call_type": pending_call["call_type"]
                                }
                                await websocket.send_text(json.dumps(payload))
                            else:
                                active_ringing_calls.pop(str(user_id), None)
                        if websocket in clients:
                            clients[websocket]["user_email"] = user_email
                            clients[websocket]["user_id"] = user_id
                        
                        if user_email not in active_connections:
                            active_connections[user_email] = 0
                        active_connections[user_email] += 1
                        
                        if active_connections[user_email] == 1:
                            update_user_status_in_db(user_email, True)
                            await broadcast_user_status(user_email, user_id, True)
                        print(f"User {user_email} re-authed. Active connections: {active_connections[user_email]} (source: {source})")
                continue

            if message.get("type") == "read":
                reader = user_collection.find_one({"email": message["sender_email"]})
                if reader:
                    reader_id = reader["_id"]
                    sender_id = ObjectId(message["receiver_id"])
                    chat_collection.update_many(
                        {"sender_id": sender_id, "receiver_id": reader_id, "is_read": False},
                        {"$set": {"is_read": True, "read_at": datetime.now(timezone.utc)}}
                    )
                    response = {"type": "read", "reader_id": str(reader_id), "sender_id": str(sender_id)}
                    await safe_broadcast(json.dumps(response))
                continue

            if message.get("type") == "reaction":
                try:
                    message_id = message.get("message_id")
                    emoji = message.get("emoji")
                    sender_email = message.get("sender_email")
                    if not message_id or not emoji or not sender_email:
                        continue
                    msg_obj_id = ObjectId(message_id)
                    existing_msg = chat_collection.find_one({"_id": msg_obj_id})
                    if not existing_msg or existing_msg.get("is_deleted_for_everyone", False):
                        continue
                    current_reactions = existing_msg.get("reactions", {})
                    if emoji not in current_reactions:
                        current_reactions[emoji] = []
                    if sender_email in current_reactions[emoji]:
                        current_reactions[emoji].remove(sender_email)
                        if len(current_reactions[emoji]) == 0:
                            del current_reactions[emoji]
                    else:
                        current_reactions[emoji].append(sender_email)
                    chat_collection.update_one({"_id": msg_obj_id}, {"$set": {"reactions": current_reactions}})
                    response = {"type": "reaction", "message_id": message_id, "emoji": emoji, "sender_email": sender_email, "reactions": current_reactions}
                    await safe_broadcast(json.dumps(response))
                except Exception as e:
                    print(f"Reaction error: {e}")
                continue

            if message.get("type") == "edit":
                try:
                    message_id = message.get("message_id")
                    new_text = message.get("text")
                    new_iv = message.get("iv")
                    new_sender_encrypted_key = message.get("sender_encrypted_key")
                    new_receiver_encrypted_key = message.get("receiver_encrypted_key")
                    sender_email = message.get("sender_email")
                    if not message_id or not new_text or not sender_email:
                        continue
                    msg_obj_id = ObjectId(message_id)
                    existing_msg = chat_collection.find_one({"_id": msg_obj_id})
                    if not existing_msg:
                        continue
                    sender = user_collection.find_one({"email": sender_email})
                    if not sender or str(existing_msg["sender_id"]) != str(sender["_id"]):
                        continue
                    if existing_msg.get("is_deleted_for_everyone", False):
                        continue
                    sent_at = existing_msg.get("sent_at")
                    if sent_at:
                        now = datetime.now(timezone.utc)
                        if sent_at.tzinfo is None:
                            sent_at = sent_at.replace(tzinfo=timezone.utc)
                        time_diff = (now - sent_at).total_seconds()
                        if time_diff > 300:
                            continue
                    update_data = {"text": new_text, "edited_at": datetime.now(timezone.utc), "is_edited": True}
                    if new_iv:
                        update_data["iv"] = new_iv
                    if new_sender_encrypted_key:
                        update_data["sender_encrypted_key"] = new_sender_encrypted_key
                    if new_receiver_encrypted_key:
                        update_data["receiver_encrypted_key"] = new_receiver_encrypted_key
                    chat_collection.update_one({"_id": msg_obj_id}, {"$set": update_data})
                    response = {
                        "type": "edit", "message_id": message_id, "text": new_text, "iv": new_iv,
                        "sender_encrypted_key": new_sender_encrypted_key, "receiver_encrypted_key": new_receiver_encrypted_key,
                        "edited_at": datetime.now(timezone.utc).isoformat(), "is_edited": True, "sender_email": sender_email
                    }
                    await safe_broadcast(json.dumps(response))
                except Exception as e:
                    print(f"Edit error: {e}")
                continue

            if message.get("type") == "delete":
                try:
                    message_id = message.get("message_id")
                    delete_type = message.get("delete_type")
                    sender_email = message.get("sender_email")
                    if not message_id or not delete_type or not sender_email:
                        continue
                    msg_obj_id = ObjectId(message_id)
                    existing_msg = chat_collection.find_one({"_id": msg_obj_id})
                    if not existing_msg:
                        continue
                    sender = user_collection.find_one({"email": sender_email})
                    if not sender:
                        continue
                    # Only the sender can delete for everyone
                    if delete_type == "for_everyone" and str(existing_msg["sender_id"]) != str(sender["_id"]):
                        continue
                    if existing_msg.get("is_deleted_for_everyone", False):
                        continue
                    if delete_type == "for_everyone":
                        chat_collection.update_one(
                            {"_id": msg_obj_id},
                            {"$set": {"text": "deleted_message", "is_deleted_for_everyone": True, "message_type": "deleted",
                                      "iv": None, "sender_encrypted_key": None, "receiver_encrypted_key": None,
                                      "duration": 0, "reactions": {}}}
                        )
                    else:
                        deleted_for_users = existing_msg.get("deleted_for_users", [])
                        if sender_email not in deleted_for_users:
                            deleted_for_users.append(sender_email)
                        chat_collection.update_one({"_id": msg_obj_id}, {"$set": {"deleted_for_users": deleted_for_users}})
                    response = {"type": "delete", "message_id": message_id, "delete_type": delete_type, "deleted_by_email": sender_email, "is_deleted_for_everyone": delete_type == "for_everyone"}
                    await safe_broadcast(json.dumps(response))
                except Exception as e:
                    print(f"Delete error: {e}")
                continue

            sender = user_collection.find_one({"email": message["sender_email"]})
            if not sender:
                continue
            sender_id = sender["_id"]
            receiver_id = ObjectId(message["receiver_id"])
            chat_data = {
                "sender_id": sender_id, "receiver_id": receiver_id, "text": message["text"],
                "message_type": message.get("message_type", "text"), "iv": message.get("iv"),
                "sender_encrypted_key": message.get("sender_encrypted_key"), "receiver_encrypted_key": message.get("receiver_encrypted_key"),
                "is_read": False, "sent_at": datetime.now(timezone.utc), "read_at": None,
                "reply_to": message.get("reply_to"), "reactions": {}, "duration": message.get("duration", 0),
                "is_deleted_for_everyone": False, "deleted_for_users": []
            }
            result = chat_collection.insert_one(chat_data)
            message_id = str(result.inserted_id)
            reply_to_message = None
            if chat_data.get("reply_to"):
                try:
                    reply_msg = chat_collection.find_one({"_id": ObjectId(chat_data["reply_to"])})
                    if reply_msg:
                        reply_deleted_for_everyone = reply_msg.get("is_deleted_for_everyone", False)
                        reply_sender = user_collection.find_one({"_id": reply_msg["sender_id"]})
                        if reply_deleted_for_everyone:
                            reply_to_message = {
                                "_id": str(reply_msg["_id"]), "sender_id": str(reply_msg["sender_id"]),
                                "sender_name": reply_sender.get("name", "") if reply_sender else "",
                                "text": "deleted_message", "iv": None, "sender_encrypted_key": None,
                                "receiver_encrypted_key": None, "duration": 0, "is_deleted_for_everyone": True
                            }
                        else:
                            reply_to_message = {
                                "_id": str(reply_msg["_id"]), "sender_id": str(reply_msg["sender_id"]),
                                "sender_name": reply_sender.get("name", "") if reply_sender else "",
                                "text": reply_msg["text"], "iv": reply_msg.get("iv"),
                                "sender_encrypted_key": reply_msg.get("sender_encrypted_key"),
                                "receiver_encrypted_key": reply_msg.get("receiver_encrypted_key"),
                                "duration": reply_msg.get("duration", 0), "is_deleted_for_everyone": False
                            }
                except:
                    pass
            receiver = user_collection.find_one({"_id": receiver_id})
            asyncio.create_task(delayed_notification(message_id, receiver, sender))
            sent_at = chat_data["sent_at"]
            response = {
                "_id": message_id, "sender_id": str(sender_id), "receiver_id": str(receiver_id),
                "receiver_email": receiver["email"], "text": message["text"],
                "message_type": message.get("message_type", "text"), "iv": message.get("iv"),
                "sender_encrypted_key": message.get("sender_encrypted_key"), "receiver_encrypted_key": message.get("receiver_encrypted_key"),
                "is_read": False, "sent_at": sent_at.isoformat(), "reply_to": reply_to_message,
                "reactions": {}, "duration": message.get("duration", 0), "is_deleted_for_me": False,
                "is_deleted_for_everyone": False, "is_edited": False
            }
            await safe_broadcast(json.dumps(response))

    except WebSocketDisconnect:
        pass
    except Exception as e:
        print(f"WebSocket error: {e}")
    finally:
        if websocket in clients:
            user_email = clients[websocket].get("user_email")
            if user_email and user_email in active_connections:
                active_connections[user_email] -= 1
                if active_connections[user_email] == 0:
                    update_user_status_in_db(user_email, False)
                    await broadcast_user_status(user_email, user_id, False)
                    del active_connections[user_email]
                print(f"User {user_email} disconnected. Active connections left: {active_connections.get(user_email, 0)}")
            try:
                del clients[websocket]
            except Exception as e:
                print(f"Error deleting client in finally: {e}")
