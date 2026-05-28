import os
import firebase_admin
from dotenv import load_dotenv
from firebase_admin import credentials
from firebase_admin import messaging

load_dotenv()

# Load service account key path from environment variable
key_path = os.getenv("FIREBASE_SERVICE_ACCOUNT_KEY", "routes/dashboard/serviceAccountKey.json")

cred = credentials.Certificate(key_path)
firebase_admin.initialize_app(cred)


def send_push_notification(token, title, body, msg_type="chat"):

    message = messaging.Message(

        data={
            "title": title,
            "body": body,
            "type": msg_type
        },

        token=token

    )

    try:
        response = messaging.send(message)
        print("SUCCESS:", response)

    except Exception as e:
        print("FCM ERROR:", e)
