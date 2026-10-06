import os

from dotenv import load_dotenv
from librouteros import connect
from librouteros.exceptions import TrapError

load_dotenv()

MIKROTIK_HOST = os.getenv("MIKROTIK_HOST")
MIKROTIK_PORT = int(os.getenv("MIKROTIK_PORT", "8728"))
MIKROTIK_USERNAME = os.getenv("MIKROTIK_USERNAME")
MIKROTIK_PASSWORD = os.getenv("MIKROTIK_PASSWORD")


def connexion_mikrotik():
    try:
        api = connect(
            username=MIKROTIK_USERNAME,
            password=MIKROTIK_PASSWORD,
            host=MIKROTIK_HOST,
            port=MIKROTIK_PORT
        )

        return api

    except TrapError as e:
        print(f"Erreur MikroTik : {e}")
        return None

    except Exception as e:
        print(f"Erreur connexion : {e}")
        return None