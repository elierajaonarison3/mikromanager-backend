from multiprocessing import connection
from fastapi import FastAPI
from sqlalchemy import text
import socket
import hashlib
import threading
import time
from database import engine
from mikrotik import connexion_mikrotik
from pydantic import BaseModel
from datetime import datetime

app = FastAPI(
    title="MikroManager API",
    version="1.0.0"
)

SITE_REFRESH_INTERVAL = 300

NOTIFICATION_CHECK_INTERVAL = 30

DAILY_TRAFFIC_LIMIT = 2 * 1024 * 1024 * 1024


def initialiser_tables_notifications():
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS mikromanager_known_devices (
                id SERIAL PRIMARY KEY,
                mac VARCHAR(50) UNIQUE NOT NULL,
                ip VARCHAR(50),
                hostname VARCHAR(255),
                first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        connection.execute(
            text("""
                ALTER TABLE mikromanager_known_devices
            ADD COLUMN IF NOT EXISTS is_connected BOOLEAN DEFAULT FALSE
            """)
        )
        
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS mikromanager_notifications (
                id SERIAL PRIMARY KEY,
                type VARCHAR(50) NOT NULL,
                title VARCHAR(255) NOT NULL,
                message TEXT NOT NULL,
                mac VARCHAR(50),
                ip VARCHAR(50),
                hostname VARCHAR(255),
                is_read BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS mikromanager_daily_traffic (
                id SERIAL PRIMARY KEY,
                mac VARCHAR(50) NOT NULL,
                ip VARCHAR(50),
                hostname VARCHAR(255),
                traffic_date DATE NOT NULL,
                download_bytes BIGINT DEFAULT 0,
                upload_bytes BIGINT DEFAULT 0,
                last_download BIGINT DEFAULT 0,
                last_upload BIGINT DEFAULT 0,
                limit_notified BOOLEAN DEFAULT FALSE,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(mac, traffic_date)
            )
        """))
 
def verifier_connexions_appareils():
    api = None
    notifications_creees = []

    try:
        api = connexion_mikrotik()

        leases = list(
            api.path("ip", "dhcp-server", "lease").select(
                "address",
                "mac-address",
                "host-name",
                "status",
                "active-address"
            )
        )

        appareils_connectes = {}

        for lease in leases:
            mac = lease.get("mac-address")

            if not mac:
                continue

            mac = mac.upper()

            active_address = lease.get("active-address")
            status = lease.get("status")

            if active_address and status == "bound":
                appareils_connectes[mac] = {
                    "ip": active_address,
                    "hostname": lease.get("host-name") or "Appareil inconnu"
                }

        with engine.begin() as connection:

            appareils_connus = connection.execute(
                text("""
                    SELECT
                        id,
                        mac,
                        ip,
                        hostname,
                        is_connected
                    FROM mikromanager_known_devices
                """)
            ).mappings().all()

            for appareil in appareils_connus:
                mac = appareil["mac"]
                ancien_etat = bool(appareil["is_connected"])
                est_connecte = mac in appareils_connectes

                if est_connecte and not ancien_etat:

                    info = appareils_connectes[mac]

                    connection.execute(
                        text("""
                            UPDATE mikromanager_known_devices
                            SET
                                ip = :ip,
                                hostname = :hostname,
                                is_connected = TRUE
                            WHERE mac = :mac
                        """),
                        {
                            "mac": mac,
                            "ip": info["ip"],
                            "hostname": info["hostname"]
                        }
                    )

                    notification = connection.execute(
                        text("""
                            INSERT INTO mikromanager_notifications
                            (
                                type,
                                title,
                                message,
                                mac,
                                ip,
                                hostname
                            )
                            VALUES
                            (
                                'device_connected',
                                'Appareil connecté',
                                :message,
                                :mac,
                                :ip,
                                :hostname
                            )
                            RETURNING id, type, title, message
                        """),
                        {
                            "message":
                                f"{info['hostname']} ({info['ip']}) vient de se connecter au réseau.",
                            "mac": mac,
                            "ip": info["ip"],
                            "hostname": info["hostname"]
                        }
                    ).mappings().first()

                    if notification:
                        notifications_creees.append(dict(notification))

                elif not est_connecte and ancien_etat:

                    connection.execute(
                        text("""
                            UPDATE mikromanager_known_devices
                            SET
                                is_connected = FALSE
                            WHERE mac = :mac
                        """),
                        {
                            "mac": mac
                        }
                    )

                    notification = connection.execute(
                        text("""
                            INSERT INTO mikromanager_notifications
                            (
                                type,
                                title,
                                message,
                                mac,
                                ip,
                                hostname
                            )
                            VALUES
                            (
                                'device_disconnected',
                                'Appareil déconnecté',
                                :message,
                                :mac,
                                :ip,
                                :hostname
                            )
                            RETURNING id, type, title, message
                        """),
                        {
                            "message":
                                f"{appareil['hostname'] or 'Appareil inconnu'} ({appareil['ip']}) vient de se déconnecter du réseau.",
                            "mac": mac,
                            "ip": appareil["ip"],
                            "hostname": appareil["hostname"]
                        }
                    ).mappings().first()

                    if notification:
                        notifications_creees.append(dict(notification))

                elif est_connecte and ancien_etat:

                    info = appareils_connectes[mac]

                    connection.execute(
                        text("""
                            UPDATE mikromanager_known_devices
                            SET
                                ip = :ip,
                                hostname = :hostname
                            WHERE mac = :mac
                        """),
                        {
                            "mac": mac,
                            "ip": info["ip"],
                            "hostname": info["hostname"]
                        }
                    )

            return notifications_creees

    except Exception as e:
        print("ERREUR CONNEXION APPAREILS =", repr(e))
        return []

    finally:
        if api:
            try:
                api.close()
            except:
                pass
                  
def verifier_consommation_quotidienne():

    api = connexion_mikrotik()

    if api is None:
        return []

    notifications = []

    try:

        leases = api.path(
            "ip",
            "dhcp-server",
            "lease"
        )

        queues = api.path(
            "queue",
            "simple"
        )

        lease_list = list(leases)
        queue_list = list(queues)

        queue_by_ip = {}

        for queue in queue_list:

            target = queue.get(
                "target",
                ""
            )

            if target:

                ip = target.split("/")[0]

                queue_by_ip[ip] = queue

        today = datetime.now().date()

        with engine.begin() as connection:

            for lease in lease_list:

                ip = lease.get("address")

                if not ip:
                    continue

                mac = lease.get("mac-address")

                if not mac:
                    continue

                mac = mac.upper()

                hostname = (
                    lease.get("host-name")
                    or "Appareil inconnu"
                )

                queue = queue_by_ip.get(ip)

                if queue is None:
                    continue

                bytes_value = queue.get(
                    "bytes",
                    "0/0"
                )

                try:

                    parts = bytes_value.split("/")

                    if len(parts) != 2:
                        continue

                    current_download = int(
                        parts[0]
                    )

                    current_upload = int(
                        parts[1]
                    )

                except (
                    ValueError,
                    TypeError
                ):

                    continue

                daily = connection.execute(
                    text("""
                        SELECT
                            id,
                            download_bytes,
                            upload_bytes,
                            last_download,
                            last_upload,
                            limit_notified
                        FROM mikromanager_daily_traffic
                        WHERE mac = :mac
                        AND traffic_date = :traffic_date
                        LIMIT 1
                    """),
                    {
                        "mac": mac,
                        "traffic_date": today
                    }
                ).mappings().first()

                if daily is None:

                    connection.execute(
                        text("""
                            INSERT INTO mikromanager_daily_traffic
                            (
                                mac,
                                ip,
                                hostname,
                                traffic_date,
                                download_bytes,
                                upload_bytes,
                                last_download,
                                last_upload,
                                limit_notified
                            )
                            VALUES
                            (
                                :mac,
                                :ip,
                                :hostname,
                                :traffic_date,
                                0,
                                0,
                                :last_download,
                                :last_upload,
                                FALSE
                            )
                        """),
                        {
                            "mac": mac,
                            "ip": ip,
                            "hostname": hostname,
                            "traffic_date": today,
                            "last_download": current_download,
                            "last_upload": current_upload
                        }
                    )

                    continue

                previous_download = int(
                    daily["last_download"] or 0
                )

                previous_upload = int(
                    daily["last_upload"] or 0
                )

                download_difference = (
                    current_download
                    - previous_download
                )

                upload_difference = (
                    current_upload
                    - previous_upload
                )

                if download_difference < 0:
                    download_difference = current_download

                if upload_difference < 0:
                    upload_difference = current_upload

                daily_download = int(
                    daily["download_bytes"] or 0
                )

                daily_upload = int(
                    daily["upload_bytes"] or 0
                )

                new_download = (
                    daily_download
                    + download_difference
                )

                new_upload = (
                    daily_upload
                    + upload_difference
                )

                total_traffic = (
                    new_download
                    + new_upload
                )

                limit_notified = bool(
                    daily["limit_notified"]
                )

                if (
                    total_traffic >= DAILY_TRAFFIC_LIMIT
                    and not limit_notified
                ):

                    total_gb = (
                        total_traffic
                        / (1024 ** 3)
                    )

                    notification = connection.execute(
                        text("""
                            INSERT INTO mikromanager_notifications
                            (
                                type,
                                title,
                                message,
                                mac,
                                ip,
                                hostname
                            )
                            VALUES
                            (
                                'traffic_limit',
                                'Limite de consommation dépassée',
                                :message,
                                :mac,
                                :ip,
                                :hostname
                            )
                            RETURNING
                                id,
                                type,
                                title,
                                message,
                                mac,
                                ip,
                                hostname,
                                is_read,
                                created_at
                        """),
                        {
                            "message": (
                                f"{hostname} a dépassé "
                                f"2 GB de consommation aujourd'hui "
                                f"({total_gb:.2f} GB)."
                            ),
                            "mac": mac,
                            "ip": ip,
                            "hostname": hostname
                        }
                    ).mappings().first()

                    connection.execute(
                        text("""
                            UPDATE mikromanager_daily_traffic
                            SET
                                download_bytes = :download_bytes,
                                upload_bytes = :upload_bytes,
                                last_download = :last_download,
                                last_upload = :last_upload,
                                limit_notified = TRUE,
                                ip = :ip,
                                hostname = :hostname,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE id = :id
                        """),
                        {
                            "download_bytes": new_download,
                            "upload_bytes": new_upload,
                            "last_download": current_download,
                            "last_upload": current_upload,
                            "ip": ip,
                            "hostname": hostname,
                            "id": daily["id"]
                        }
                    )

                    if notification:

                        item = dict(notification)

                        if item.get("created_at"):
                            item["created_at"] = (
                                item["created_at"].isoformat()
                            )

                        notifications.append(item)

                else:

                    connection.execute(
                        text("""
                            UPDATE mikromanager_daily_traffic
                            SET
                                download_bytes = :download_bytes,
                                upload_bytes = :upload_bytes,
                                last_download = :last_download,
                                last_upload = :last_upload,
                                ip = :ip,
                                hostname = :hostname,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE id = :id
                        """),
                        {
                            "download_bytes": new_download,
                            "upload_bytes": new_upload,
                            "last_download": current_download,
                            "last_upload": current_upload,
                            "ip": ip,
                            "hostname": hostname,
                            "id": daily["id"]
                        }
                    )

        return notifications

    except Exception as e:

        print(
            "ERREUR SUIVI CONSOMMATION =",
            repr(e)
        )

        return []

    finally:

        api.close()

def verifier_nouveaux_appareils():
    api = connexion_mikrotik()

    if api is None:
        return []

    nouvelles_notifications = []

    try:
        leases = api.path(
            "ip",
            "dhcp-server",
            "lease"
        )

        lease_list = list(leases)

        with engine.begin() as connection:

            for lease in lease_list:

                status = lease.get("status")
                active_address = lease.get("active-address")

                if status != "bound" or not active_address:
                    continue

                mac = lease.get("mac-address")
                ip = lease.get("address")
                hostname = lease.get("host-name") or "Appareil inconnu"

                if not mac:
                    continue

                mac = mac.upper()

                appareil_existe = connection.execute(
                    text("""
                        SELECT id
                        FROM mikromanager_known_devices
                        WHERE mac = :mac
                        LIMIT 1
                    """),
                    {
                        "mac": mac
                    }
                ).fetchone()

                if appareil_existe:
                    connection.execute(
                        text("""
                            UPDATE mikromanager_known_devices
                            SET ip = :ip,
                                hostname = :hostname
                            WHERE mac = :mac
                        """),
                        {
                            "mac": mac,
                            "ip": ip,
                            "hostname": hostname
                        }
                    )

                    continue

                connection.execute(
                    text("""
                        INSERT INTO mikromanager_known_devices
                        (
                            mac,
                            ip,
                            hostname
                        )
                        VALUES
                        (
                            :mac,
                            :ip,
                            :hostname
                        )
                    """),
                    {
                        "mac": mac,
                        "ip": ip,
                        "hostname": hostname
                    }
                )

                notification = connection.execute(
                    text("""
                        INSERT INTO mikromanager_notifications
                        (
                            type,
                            title,
                            message,
                            mac,
                            ip,
                            hostname
                        )
                        VALUES
                        (
                            'new_device',
                            'Nouvel appareil détecté',
                            :message,
                            :mac,
                            :ip,
                            :hostname
                        )
                        RETURNING id, type, title, message,
                                  mac, ip, hostname, is_read,
                                  created_at
                    """),
                    {
                        "message": (
                            f"Nouvel appareil connecté : "
                            f"{hostname} ({ip})"
                        ),
                        "mac": mac,
                        "ip": ip,
                        "hostname": hostname
                    }
                ).mappings().first()

                if notification:
                    nouvelle_notification = dict(notification)

                    if nouvelle_notification.get("created_at"):
                        nouvelle_notification["created_at"] = (
                            nouvelle_notification["created_at"].isoformat()
                        )

                    nouvelles_notifications.append(
                        nouvelle_notification
                    )

        return nouvelles_notifications

    except Exception as e:
        print(
            "ERREUR DETECTION NOUVEL APPAREIL =",
            repr(e)
        )

        return []

    finally:
        api.close()
        
def boucle_verification_notifications():
    print("Service automatique des notifications démarré")

    while True:
        try:
            nouvelles_notifications = verifier_nouveaux_appareils()

            if nouvelles_notifications:
                print(
                    "Nouveaux appareils détectés :",
                    len(nouvelles_notifications)
                )

            connexions_notifications = verifier_connexions_appareils()

            if connexions_notifications:
                print(
                    "Changements de connexion détectés :",
                    len(connexions_notifications)
                )

            traffic_notifications = verifier_consommation_quotidienne()

            if traffic_notifications:
                print(
                    "Limites de consommation détectées :",
                    len(traffic_notifications)
                )

        except Exception as e:
            print(
                "ERREUR BOUCLE NOTIFICATIONS =",
                repr(e)
            )

        time.sleep(NOTIFICATION_CHECK_INTERVAL)
class LimitRequest(BaseModel):
    download: int
    upload: int


class ServiceBlockRequest(BaseModel):
    mac: str
    domain: str


@app.get("/")
def accueil():
    return {
        "message": "MikroManager API fonctionne"
    }


@app.get("/test-database")
def test_database():
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))

        return {
            "status": "success",
            "message": "Connexion PostgreSQL réussie"
        }

    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }


@app.get("/test-mikrotik")
def test_mikrotik():
    api = connexion_mikrotik()

    if api is None:
        return {
            "status": "error",
            "message": "Connexion MikroTik échouée"
        }

    try:
        resource = api.path(
            "system",
            "resource"
        )

        data = list(resource)

        if not data:
            return {
                "status": "error",
                "message": "Aucune donnée reçue du MikroTik"
            }

        info = data[0]

        return {
            "status": "success",
            "message": "Connexion MikroTik réussie",
            "cpu": info.get("cpu-load"),
            "ram_total": info.get("total-memory"),
            "ram_free": info.get("free-memory"),
            "uptime": info.get("uptime"),
            "version": info.get("version")
        }

    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }

    finally:
        api.close()


@app.get("/mikrotik/devices")
def mikrotik_devices():
    api = connexion_mikrotik()

    if api is None:
        return {
            "status": "error",
            "message": "Connexion MikroTik échouée"
        }

    try:
        leases = api.path(
            "ip",
            "dhcp-server",
            "lease"
        )

        data = list(leases)

        result = []

        for lease in data:

            status = lease.get("status")
            active_address = lease.get("active-address")

            if status == "bound" and active_address:
                device_status = "connected"
            else:
                device_status = "inactive"

            result.append({
                "ip": lease.get("address"),
                "mac": lease.get("mac-address"),
                "hostname": lease.get("host-name"),
                "status": device_status,
                "last_seen": lease.get("last-seen")
            })

        connected_devices = [
            device
            for device in result
            if device["status"] == "connected"
        ]

        return {
            "status": "success",
            "total": len(result),
            "connected": len(connected_devices),
            "devices": result
        }

    except Exception as e:

        return {
            "status": "error",
            "message": str(e)
        }

    finally:
        api.close()


@app.get("/mikrotik/devices/traffic")
def mikrotik_devices_traffic():
    api = connexion_mikrotik()

    if api is None:
        return {
            "status": "error",
            "message": "Connexion MikroTik échouée"
        }

    try:
        leases = api.path(
            "ip",
            "dhcp-server",
            "lease"
        )

        queues = api.path(
            "queue",
            "simple"
        )

        lease_list = list(leases)
        queue_list = list(queues)

        queue_by_ip = {}

        for queue in queue_list:

            target = queue.get(
                "target",
                ""
            )

            if target:
                ip = target.split("/")[0]
                queue_by_ip[ip] = queue

        result = []

        for lease in lease_list:

            ip = lease.get("address")

            if not ip:
                continue

            status = lease.get("status")
            active_address = lease.get("active-address")

            if status == "bound" and active_address:
                device_status = "connected"
            else:
                device_status = "inactive"

            queue = queue_by_ip.get(ip)

            download = 0
            upload = 0
            rate_download = 0
            rate_upload = 0

            if queue:

                bytes_value = queue.get(
                    "bytes",
                    "0/0"
                )

                rate_value = queue.get(
                    "rate",
                    "0/0"
                )

                try:
                    byte_parts = bytes_value.split("/")

                    download = int(
                        byte_parts[0]
                    )

                    upload = int(
                        byte_parts[1]
                    )

                except Exception:

                    download = 0
                    upload = 0

                try:
                    rate_parts = rate_value.split("/")

                    rate_download = int(
                        rate_parts[0]
                    )

                    rate_upload = int(
                        rate_parts[1]
                    )

                except Exception:

                    rate_download = 0
                    rate_upload = 0

            result.append({
                "ip": ip,
                "mac": lease.get("mac-address"),
                "hostname": lease.get("host-name"),
                "interface": None,
                "status": device_status,
                "download": download,
                "upload": upload,
                "rate_download": rate_download,
                "rate_upload": rate_upload,
                "last_seen": lease.get("last-seen")
            })

        connected_devices = [
            device
            for device in result
            if device["status"] == "connected"
        ]

        return {
            "status": "success",
            "total": len(result),
            "connected": len(connected_devices),
            "devices": result
        }

    except Exception as e:

        return {
            "status": "error",
            "message": str(e),
            "type": type(e).__name__
        }

    finally:
        api.close()


@app.post("/mikrotik/devices/auto-queues")
def create_auto_queues():
    api = connexion_mikrotik()

    if api is None:
        return {
            "status": "error",
            "message": "Connexion MikroTik échouée"
        }

    try:
        leases = api.path(
            "ip",
            "dhcp-server",
            "lease"
        )

        queues = api.path(
            "queue",
            "simple"
        )

        lease_list = list(leases)
        queue_list = list(queues)

        existing_targets = set()

        for queue in queue_list:

            target = queue.get("target")

            if target:
                existing_targets.add(target)

        created = []
        already_exists = []

        for lease in lease_list:

            ip = lease.get("address")
            status = lease.get("status")
            active_address = lease.get("active-address")
            hostname = lease.get("host-name")

            if not ip:
                continue

            if status != "bound" or not active_address:
                continue

            target = f"{ip}/32"

            if target in existing_targets:

                already_exists.append({
                    "ip": ip,
                    "hostname": hostname
                })

                continue

            queue_name = f"MikroManager-{ip}"

            queues.add(
                name=queue_name,
                target=target,
                **{
                    "max-limit": "1000M/1000M"
                }
            )

            created.append({
                "ip": ip,
                "hostname": hostname,
                "queue": queue_name
            })

            existing_targets.add(target)

        return {
            "status": "success",
            "created": len(created),
            "already_exists": len(already_exists),
            "queues_created": created,
            "queues_already_exists": already_exists
        }

    except Exception as e:

        return {
            "status": "error",
            "message": str(e),
            "type": type(e).__name__
        }

    finally:
        api.close()


@app.get("/mikrotik/dhcp-leases")
def mikrotik_dhcp_leases():
    api = connexion_mikrotik()

    if api is None:
        return {
            "status": "error",
            "message": "Connexion MikroTik échouée"
        }

    try:
        leases = api.path(
            "ip",
            "dhcp-server",
            "lease"
        )

        data = list(leases)

        result = []

        for lease in data:

            result.append({
                "ip": lease.get("address"),
                "mac": lease.get("mac-address"),
                "hostname": lease.get("host-name"),
                "status": lease.get("status"),
                "active": lease.get("active-address"),
                "last_seen": lease.get("last-seen")
            })

        return {
            "status": "success",
            "total": len(result),
            "leases": result
        }

    except Exception as e:

        return {
            "status": "error",
            "message": str(e)
        }

    finally:
        api.close()

@app.get("/mikrotik/notifications")
def get_notifications():
    nouvelles_notifications = verifier_nouveaux_appareils()

    try:
        with engine.connect() as connection:

            notifications = connection.execute(
                text("""
                    SELECT
                        id,
                        type,
                        title,
                        message,
                        mac,
                        ip,
                        hostname,
                        is_read,
                        created_at
                    FROM mikromanager_notifications
                    WHERE is_read = FALSE
                    ORDER BY created_at DESC
                """)
            ).mappings().all()

            result = []

            for notification in notifications:

                item = dict(notification)

                if item.get("created_at"):
                    item["created_at"] = (
                        item["created_at"].isoformat()
                    )

                result.append(item)

            return {
                "status": "success",
                "new_detected": len(nouvelles_notifications),
                "total_unread": len(result),
                "notifications": result
            }

    except Exception as e:

        return {
            "status": "error",
            "message": str(e)
        }

@app.put("/mikrotik/notifications/{notification_id}/read")
def mark_notification_as_read(notification_id: int):
    try:
        with engine.begin() as connection:
            result = connection.execute(
                text("""
                    UPDATE mikromanager_notifications
                    SET is_read = TRUE
                    WHERE id = :id
                """),
                {
                    "id": notification_id
                }
            )

            if result.rowcount == 0:
                return {
                    "status": "error",
                    "message": "Notification introuvable"
                }

            return {
                "status": "success",
                "message": "Notification marquée comme lue"
            }

    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }

@app.get("/mikrotik/queues")
def mikrotik_queues():
    api = connexion_mikrotik()

    if api is None:
        return {
            "status": "error",
            "message": "Connexion MikroTik échouée"
        }

    try:
        queues = api.path(
            "queue",
            "simple"
        )

        data = list(queues)

        result = []

        for queue in data:

            result.append({
                "name": queue.get("name"),
                "target": queue.get("target"),
                "max_limit": queue.get("max-limit"),
                "bytes": queue.get("bytes"),
                "packets": queue.get("packets"),
                "rate": queue.get("rate")
            })

        return {
            "status": "success",
            "total": len(result),
            "queues": result
        }

    except Exception as e:

        return {
            "status": "error",
            "message": str(e)
        }

    finally:
        api.close()


@app.post("/mikrotik/queues/test")
def create_test_queue():
    api = connexion_mikrotik()

    if api is None:
        return {
            "status": "error",
            "message": "Connexion MikroTik échouée"
        }

    try:
        queues = api.path(
            "queue",
            "simple"
        )

        queues.add(
            name="test-realme-c2",
            target="10.5.50.60/32",
            **{
                "max-limit": "5M/5M"
            }
        )

        return {
            "status": "success",
            "message": "Simple Queue créée",
            "queue": "test-realme-c2",
            "target": "10.5.50.60/32",
            "limit": "5 Mbps"
        }

    except Exception as e:

        return {
            "status": "error",
            "message": str(e)
        }

    finally:
        api.close()


@app.put("/mikrotik/devices/{ip}/limit")
def limit_device(
    ip: str,
    request: LimitRequest
):
    api = connexion_mikrotik()

    if api is None:
        return {
            "status": "error",
            "message": "Connexion MikroTik échouée"
        }

    try:
        queues = api.path(
            "queue",
            "simple"
        )

        queue_list = list(queues)

        target = f"{ip}/32"

        queue = None

        for item in queue_list:

            if item.get("target") == target:
                queue = item
                break

        if queue is None:

            return {
                "status": "error",
                "message": f"Aucune queue trouvée pour {ip}"
            }

        download = request.download
        upload = request.upload

        if download <= 0 or upload <= 0:

            return {
                "status": "error",
                "message": "La limite doit être supérieure à 0 Mbps"
            }

        queues.update(
            **{
                ".id": queue[".id"],
                "max-limit": f"{upload}M/{download}M"
            }
        )

        return {
            "status": "success",
            "message": "Limite de débit mise à jour",
            "ip": ip,
            "download": f"{download} Mbps",
            "upload": f"{upload} Mbps"
        }

    except Exception as e:

        return {
            "status": "error",
            "message": str(e)
        }

    finally:
        api.close()


@app.put("/mikrotik/devices/{ip}/block")
def block_device(ip: str):
    api = connexion_mikrotik()

    if api is None:
        return {
            "status": "error",
            "message": "Connexion MikroTik échouée"
        }

    try:
        queues = api.path(
            "queue",
            "simple"
        )

        queue_list = list(queues)

        target = f"{ip}/32"

        queue = None

        for item in queue_list:

            if item.get("target") == target:
                queue = item
                break

        if queue is None:

            return {
                "status": "error",
                "message": f"Aucune queue trouvée pour {ip}"
            }

        queues.update(
            **{
                ".id": queue[".id"],
                "max-limit": "1/1"
            }
        )

        return {
            "status": "success",
            "message": "Internet bloqué",
            "ip": ip
        }

    except Exception as e:

        return {
            "status": "error",
            "message": str(e)
        }

    finally:
        api.close()


@app.put("/mikrotik/devices/{ip}/firewall-block")
def firewall_block_device(ip: str):
    api = connexion_mikrotik()

    if api is None:
        return {
            "status": "error",
            "message": "Connexion MikroTik échouée"
        }

    try:
        firewall = api.path(
            "ip",
            "firewall",
            "filter"
        )

        rules = list(firewall)

        target_rule = None

        for rule in rules:

            if (
                rule.get("chain") == "forward"
                and rule.get("action") == "drop"
                and rule.get("src-address") == ip
            ):
                target_rule = rule
                break

        if target_rule is not None:

            disabled = target_rule.get("disabled")

            if disabled in [
                "yes",
                "true",
                True
            ]:

                firewall.update(
                    **{
                        ".id": target_rule[".id"],
                        "disabled": "no"
                    }
                )

                return {
                    "status": "success",
                    "message": "Internet bloqué",
                    "ip": ip,
                    "rule_id": target_rule[".id"]
                }

            return {
                "status": "success",
                "message": "Appareil déjà bloqué",
                "ip": ip,
                "rule_id": target_rule[".id"]
            }

        new_rule = firewall.add(
            chain="forward",
            action="drop",
            **{
                "src-address": ip,
                "place-before": "0"
            }
        )

        return {
            "status": "success",
            "message": "Règle Firewall créée",
            "ip": ip,
            "rule_id": new_rule.get(".id")
        }

    except Exception as e:

        return {
            "status": "error",
            "message": str(e),
            "type": type(e).__name__
        }

    finally:
        api.close()


@app.put("/mikrotik/devices/{ip}/unblock")
def unblock_device(ip: str):
    api = connexion_mikrotik()

    if api is None:
        return {
            "status": "error",
            "message": "Connexion MikroTik échouée"
        }

    try:
        queues = api.path(
            "queue",
            "simple"
        )

        queue_list = list(queues)

        target = f"{ip}/32"

        queue = None

        for item in queue_list:

            if item.get("target") == target:
                queue = item
                break

        if queue is None:

            return {
                "status": "error",
                "message": f"Aucune queue trouvée pour {ip}"
            }

        queues.update(
            **{
                ".id": queue[".id"],
                "max-limit": "1000M/1000M"
            }
        )

        return {
            "status": "success",
            "message": "Internet débloqué",
            "ip": ip
        }

    except Exception as e:

        return {
            "status": "error",
            "message": str(e)
        }

    finally:
        api.close()


@app.put("/mikrotik/devices/{ip}/firewall-unblock")
def firewall_unblock_device(ip: str):
    api = connexion_mikrotik()

    if api is None:
        return {
            "status": "error",
            "message": "Connexion MikroTik échouée"
        }

    try:
        firewall = api.path(
            "ip",
            "firewall",
            "filter"
        )

        rules = list(firewall)

        target_rule = None

        for rule in rules:

            if (
                rule.get("chain") == "forward"
                and rule.get("action") == "drop"
                and rule.get("src-address") == ip
            ):
                target_rule = rule
                break

        if target_rule is None:

            return {
                "status": "success",
                "message": "Aucune règle de blocage trouvée",
                "ip": ip
            }

        disabled = target_rule.get("disabled")

        if disabled in [
            "yes",
            "true",
            True
        ]:

            return {
                "status": "success",
                "message": "Appareil déjà débloqué",
                "ip": ip,
                "rule_id": target_rule[".id"]
            }

        firewall.update(
            **{
                ".id": target_rule[".id"],
                "disabled": "yes"
            }
        )

        return {
            "status": "success",
            "message": "Internet débloqué",
            "ip": ip,
            "rule_id": target_rule[".id"]
        }

    except Exception as e:

        return {
            "status": "error",
            "message": str(e),
            "type": type(e).__name__
        }

    finally:
        api.close()


@app.get("/mikrotik/firewall")
def mikrotik_firewall():
    api = connexion_mikrotik()

    if api is None:
        return {
            "status": "error",
            "message": "Connexion MikroTik échouée"
        }

    try:
        firewall = api.path(
            "ip",
            "firewall",
            "filter"
        )

        rules = list(firewall)

        result = []

        for rule in rules:

            result.append({
                "id": rule.get(".id"),
                "chain": rule.get("chain"),
                "action": rule.get("action"),
                "src_address": rule.get("src-address"),
                "dst_address": rule.get("dst-address"),
                "comment": rule.get("comment"),
                "disabled": rule.get("disabled"),
                "bytes": rule.get("bytes"),
                "packets": rule.get("packets")
            })

        return {
            "status": "success",
            "total": len(result),
            "rules": result
        }

    except Exception as e:

        return {
            "status": "error",
            "message": str(e)
        }

    finally:
        api.close()


@app.get("/mikrotik/dashboard")
def mikrotik_dashboard():
    api = connexion_mikrotik()

    if api is None:
        return {
            "status": "error",
            "message": "Connexion MikroTik échouée"
        }

    try:
        resource = api.path(
            "system",
            "resource"
        )

        leases = api.path(
            "ip",
            "dhcp-server",
            "lease"
        )

        queues = api.path(
            "queue",
            "simple"
        )

        resource_list = list(resource)
        lease_list = list(leases)
        queue_list = list(queues)

        if not resource_list:

            return {
                "status": "error",
                "message": "Informations système MikroTik introuvables"
            }

        system = resource_list[0]

        connected = 0

        for lease in lease_list:

            if (
                lease.get("status") == "bound"
                and lease.get("active-address")
            ):
                connected += 1

        total_download = 0
        total_upload = 0

        for queue in queue_list:

            bytes_value = queue.get(
                "bytes",
                "0/0"
            )

            try:

                parts = bytes_value.split("/")

                if len(parts) == 2:

                    total_upload += int(
                        parts[0]
                    )

                    total_download += int(
                        parts[1]
                    )

            except (
                ValueError,
                TypeError
            ):
                continue

        total_devices = len(
            lease_list
        )

        return {
            "status": "success",
            "mikrotik": {
                "online": True,
                "cpu_load": int(
                    system.get(
                        "cpu-load",
                        0
                    )
                ),
                "free_memory": int(
                    system.get(
                        "free-memory",
                        0
                    )
                ),
                "total_memory": int(
                    system.get(
                        "total-memory",
                        0
                    )
                ),
                "uptime": system.get(
                    "uptime"
                ),
                "version": system.get(
                    "version"
                ),
                "board_name": system.get(
                    "board-name"
                ),
                "architecture": system.get(
                    "architecture-name"
                )
            },
            "devices": {
                "total": total_devices,
                "connected": connected,
                "inactive": (
                    total_devices - connected
                )
            },
            "traffic": {
                "total_download": total_download,
                "total_upload": total_upload
            }
        }

    except Exception as e:

        return {
            "status": "error",
            "message": str(e),
            "type": type(e).__name__
        }

    finally:
        api.close()


def normaliser_domaine(domain: str):

    domain = domain.strip().lower()

    domain = domain.replace(
        "https://",
        ""
    )

    domain = domain.replace(
        "http://",
        ""
    )

    domain = domain.split("/")[0]

    domain = domain.split("?")[0]

    domain = domain.split("#")[0]

    if domain.startswith("www."):
        domain = domain[4:]

    return domain.strip()

def extraire_ip_adresse(valeur):
    if not valeur:
        return None

    valeur = str(valeur)

    if ":" in valeur:
        valeur = valeur.rsplit(":", 1)[0]

    if "/" in valeur:
        valeur = valeur.split("/", 1)[0]

    return valeur.strip()


def est_ip_privee(ip):
    if not ip:
        return False

    parties = ip.split(".")

    if len(parties) != 4:
        return False

    try:
        a, b, c, d = [int(x) for x in parties]
    except ValueError:
        return False

    if a == 10:
        return True

    if a == 192 and b == 168:
        return True

    if a == 172 and 16 <= b <= 31:
        return True

    if a == 127:
        return True

    return False


def nettoyer_nom_site(domain):
    if not domain:
        return None

    domain = str(domain).strip().lower()

    if domain.endswith("."):
        domain = domain[:-1]

    if domain.startswith("www."):
        domain = domain[4:]

    return domain



def nettoyer_nom_site(domain):
    if not domain:
        return None

    domain = str(domain).strip().lower()

    if domain.endswith("."):
        domain = domain[:-1]

    if domain.startswith("www."):
        domain = domain[4:]

    return domain


def nom_site_depuis_domaine(domain):
    domain = nettoyer_nom_site(domain)

    if not domain:
        return None

    sites = {
        "youtube.com": "YouTube",
        "youtube-nocookie.com": "YouTube",
        "googlevideo.com": "YouTube",
        "ytimg.com": "YouTube",

        "facebook.com": "Facebook",
        "fbcdn.net": "Facebook",

        "instagram.com": "Instagram",
        "cdninstagram.com": "Instagram",

        "tiktok.com": "TikTok",
        "tiktokcdn.com": "TikTok",
        "byteoversea.net": "TikTok",

        "whatsapp.com": "WhatsApp",
        "whatsapp.net": "WhatsApp",

        "telegram.org": "Telegram",
        "telegram.me": "Telegram",

        "twitter.com": "X",
        "x.com": "X",

        "netflix.com": "Netflix",

        "spotify.com": "Spotify",

        "chatgpt.com": "ChatGPT",
        "openai.com": "OpenAI",

        "anydesk.com": "AnyDesk",
        "net.anydesk.com": "AnyDesk",

        "microsoft.com": "Microsoft",
        "live.com": "Microsoft",
        "office.com": "Microsoft",

        "apple.com": "Apple",

        "amazon.com": "Amazon",
    }

    for domaine, nom in sites.items():

        if domain == domaine:
            return nom

        if domain.endswith("." + domaine):
            return nom

    return None


def est_domaine_technique(domain):
    if not domain:
        return True

    domain = nettoyer_nom_site(domain)

    domaines_techniques = [
        "googleapis.com",
        "googleusercontent.com",
        "gstatic.com",
        "googleadservices.com",
        "doubleclick.net",

        "cloudflare.com",
        "cloudflare.net",

        "akamai.net",
        "akamaiedge.net",
        "akamaihd.net",

        "usercentrics.eu",

        "microsoftonline.com",
        "msftconnecttest.com",
        "msftncsi.com",

        "pki-goog.l.google.com",
        "safebrowsing.googleapis.com",

        "update.googleapis.com",
    ]

    for domaine in domaines_techniques:

        if domain == domaine:
            return True

        if domain.endswith("." + domaine):
            return True

    return False


def convertir_octets(octets):
    if octets is None:
        return 0

    try:
        return int(octets)
    except:
        return 0


def format_octets(octets):
    octets = convertir_octets(octets)

    if octets < 1024:
        return f"{octets} B"

    if octets < 1024 * 1024:
        return f"{octets / 1024:.1f} KB"

    if octets < 1024 * 1024 * 1024:
        return f"{octets / (1024 * 1024):.1f} MB"

    return f"{octets / (1024 * 1024 * 1024):.2f} GB"


def obtenir_sites_detectes(api):

    leases_path = api.path(
        "ip",
        "dhcp-server",
        "lease"
    )

    connections_path = api.path(
        "ip",
        "firewall",
        "connection"
    )

    dns_cache_path = api.path(
        "ip",
        "dns",
        "cache"
    )

    leases = list(leases_path)
    connections = list(connections_path)
    dns_cache = list(dns_cache_path)

    appareils = {}

    for lease in leases:

        status = lease.get("status")
        active_address = lease.get("active-address")

        if status != "bound" or not active_address:
            continue

        ip = lease.get("address")

        if not ip:
            continue

        mac = lease.get("mac-address")

        if mac:
            mac = mac.upper()

        hostname = (
            lease.get("host-name")
            or "Appareil inconnu"
        )

        appareils[ip] = {
            "ip": ip,
            "mac": mac,
            "hostname": hostname,
            "connected": True,
            "total_bytes": 0,
            "sites": {}
        }

    dns_by_ip = {}

    for entry in dns_cache:

        domain = entry.get("name")

        if not domain:
            continue

        domain = nettoyer_nom_site(domain)

        if not domain:
            continue

        addresses = []

        address = entry.get("address")

        if address:
            ip = extraire_ip_adresse(address)

            if ip:
                addresses.append(ip)

        data = entry.get("data")

        if data:
            ip = extraire_ip_adresse(data)

            if ip:
                addresses.append(ip)

        for ip in addresses:

            if not ip:
                continue

            if est_ip_privee(ip):
                continue

            if ip not in dns_by_ip:
                dns_by_ip[ip] = set()

            dns_by_ip[ip].add(domain)

    for connection in connections:

        src_ip = extraire_ip_adresse(
            connection.get("src-address")
        )

        dst_ip = extraire_ip_adresse(
            connection.get("dst-address")
        )

        if not src_ip or not dst_ip:
            continue

        if src_ip not in appareils:
            continue

        if est_ip_privee(dst_ip):
            continue

        orig_bytes = convertir_octets(
            connection.get("orig-bytes")
        )

        repl_bytes = convertir_octets(
            connection.get("repl-bytes")
        )

        total_connection = (
            orig_bytes + repl_bytes
        )

        appareils[src_ip]["total_bytes"] += (
            total_connection
        )

        domaines = dns_by_ip.get(
            dst_ip,
            set()
        )

        domaines_valides = []

        for domain in domaines:

            domain = nettoyer_nom_site(domain)

            if not domain:
                continue

            if est_domaine_technique(domain):
                continue

            nom = nom_site_depuis_domaine(
                domain
            )

            if not nom:
                continue

            domaines_valides.append(
                (nom, domain)
            )

        if not domaines_valides:
            continue

        consommation_par_site = (
            total_connection
            / len(domaines_valides)
        )

        for nom, domain in domaines_valides:

            if nom not in appareils[src_ip]["sites"]:
                appareils[src_ip]["sites"][nom] = {
                    "name": nom,
                    "domains": [],
                    "bytes": 0
                }

            if domain not in appareils[src_ip]["sites"][nom]["domains"]:
                appareils[src_ip]["sites"][nom]["domains"].append(
                    domain
                )

            appareils[src_ip]["sites"][nom]["bytes"] += (
                consommation_par_site
            )

    result = []

    for appareil in appareils.values():

        sites = list(
            appareil["sites"].values()
        )

        sites.sort(
            key=lambda x: x["bytes"],
            reverse=True
        )

        total_bytes = appareil["total_bytes"]

        result.append({
            "mac": appareil["mac"],
            "ip": appareil["ip"],
            "hostname": appareil["hostname"],
            "connected": appareil["connected"],
            "total_bytes": total_bytes,
            "total_formatted": format_octets(
                total_bytes
            ),
            "sites": [
                {
                    "name": site["name"],
                    "domains": site["domains"],
                    "bytes": int(site["bytes"]),
                    "formatted": format_octets(
                        site["bytes"]
                    )
                }
                for site in sites
            ]
        })

    result.sort(
        key=lambda x: x["total_bytes"],
        reverse=True
    )

    return result


def nom_address_list(
    mac: str,
    domain: str
):

    valeur = f"{mac}-{domain}"

    hash_value = hashlib.md5(
        valeur.encode("utf-8")
    ).hexdigest()[:10]

    return f"MM-SITE-{hash_value}"


def resoudre_domaine(domain: str):

    try:

        resultats = socket.getaddrinfo(
            domain,
            443,
            socket.AF_INET,
            socket.SOCK_STREAM
        )

        ips = set()

        for resultat in resultats:

            ip = resultat[4][0]

            if ip:
                ips.add(ip)

        return sorted(ips)

    except Exception as e:

        print(
            f"ERREUR DNS {domain} =",
            repr(e)
        )

        return []


def actualiser_site_block(
    api,
    mac,
    domain,
    list_name,
    comment
):

    try:

        ips_nouvelles = set(
            resoudre_domaine(domain)
        )

        if not ips_nouvelles:

            print(
                f"Aucune IP trouvée pour {domain}"
            )

            return {
                "domain": domain,
                "status": "no_ip"
            }

        address_lists = api.path(
            "ip",
            "firewall",
            "address-list"
        )

        addresses = list(
            address_lists
        )

        anciennes_ips = set()

        for address in addresses:

            if (
                address.get("list") == list_name
                and address.get("comment") == comment
            ):

                ip = address.get("address")

                if ip:
                    anciennes_ips.add(ip)

        ips_a_ajouter = (
            ips_nouvelles - anciennes_ips
        )

        ips_a_supprimer = (
            anciennes_ips - ips_nouvelles
        )

        for ip in ips_a_ajouter:

            address_lists.add(
                address=ip,
                list=list_name,
                comment=comment
            )

        for address in addresses:

            ip = address.get("address")

            if (
                address.get("list") == list_name
                and address.get("comment") == comment
                and ip in ips_a_supprimer
            ):

                address_lists.remove(
                    address[".id"]
                )

        print(
            f"Site actualisé : {domain}"
        )

        print(
            f"Anciennes IP : {anciennes_ips}"
        )

        print(
            f"Nouvelles IP : {ips_nouvelles}"
        )

        print(
            f"Ajoutées : {ips_a_ajouter}"
        )

        print(
            f"Supprimées : {ips_a_supprimer}"
        )

        return {
            "domain": domain,
            "status": "updated",
            "ips": sorted(ips_nouvelles),
            "added": sorted(ips_a_ajouter),
            "removed": sorted(ips_a_supprimer)
        }

    except Exception as e:

        print(
            f"ERREUR ACTUALISATION {domain} =",
            repr(e)
        )

        return {
            "domain": domain,
            "status": "error",
            "message": str(e)
        }


def actualiser_tous_les_sites():

    api = connexion_mikrotik()

    if api is None:

        print(
            "Actualisation sites : connexion MikroTik échouée"
        )

        return

    try:

        firewall = api.path(
            "ip",
            "firewall",
            "filter"
        )

        rules = list(
            firewall
        )

        sites = []

        for rule in rules:

            comment = rule.get(
                "comment",
                ""
            )

            if not comment.startswith(
                "MikroManager-SITE-"
            ):
                continue

            prefix = "MikroManager-SITE-"

            valeur = comment[
                len(prefix):
            ]

            separateur = valeur.find("-")

            if separateur == -1:
                continue

            mac = valeur[
                :separateur
            ]

            domain = valeur[
                separateur + 1:
            ]

            list_name = nom_address_list(
                mac,
                domain
            )

            sites.append({
                "mac": mac,
                "domain": domain,
                "list_name": list_name,
                "comment": comment
            })

        print(
            f"Sites à actualiser : {len(sites)}"
        )

        resultats = []

        for site in sites:

            resultat = actualiser_site_block(
                api,
                site["mac"],
                site["domain"],
                site["list_name"],
                site["comment"]
            )

            resultats.append(
                resultat
            )

        return resultats

    except Exception as e:

        print(
            "ERREUR ACTUALISATION SITES =",
            repr(e)
        )

    finally:
        api.close()


def boucle_actualisation_sites():

    print(
        "Service automatique des sites démarré"
    )

    while True:

        try:

            actualiser_tous_les_sites()

        except Exception as e:

            print(
                "ERREUR BOUCLE SITES =",
                repr(e)
            )

        time.sleep(
            SITE_REFRESH_INTERVAL
        )


@app.on_event("startup")
def demarrer_services_automatiques():

    initialiser_tables_notifications()

    thread_sites = threading.Thread(
        target=boucle_actualisation_sites,
        daemon=True
    )

    thread_sites.start()

    thread_notifications = threading.Thread(
        target=boucle_verification_notifications,
        daemon=True
    )

    thread_notifications.start()


@app.put("/mikrotik/devices/{mac}/site-block")
def block_site_device(
    mac: str,
    request: ServiceBlockRequest
):

    api = connexion_mikrotik()

    if api is None:

        return {
            "status": "error",
            "message": "Connexion MikroTik échouée"
        }

    try:

        mac = mac.strip().upper()

        domain = normaliser_domaine(
            request.domain
        )

        if not mac:

            return {
                "status": "error",
                "message": "Adresse MAC manquante"
            }

        if not domain:

            return {
                "status": "error",
                "message": "Domaine invalide"
            }

        ips = resoudre_domaine(
            domain
        )

        print(
            "Domaine à bloquer :",
            domain
        )

        print(
            "IPs trouvées :",
            ips
        )

        if not ips:

            return {
                "status": "error",
                "message": (
                    f"Impossible de résoudre "
                    f"le domaine {domain}"
                )
            }

        firewall = api.path(
            "ip",
            "firewall",
            "filter"
        )

        address_lists = api.path(
            "ip",
            "firewall",
            "address-list"
        )

        list_name = nom_address_list(
            mac,
            domain
        )

        comment = (
            f"MikroManager-SITE-{mac}-{domain}"
        )

        existing_rules = list(
            firewall
        )

        for rule in existing_rules:

            if rule.get("comment") == comment:

                return {
                    "status": "error",
                    "message": (
                        "Ce domaine est déjà "
                        "bloqué pour cet appareil"
                    )
                }

        existing_addresses = list(
            address_lists
        )

        addresses_added = []

        for ip in ips:

            deja_present = False

            for address in existing_addresses:

                if (
                    address.get("list") == list_name
                    and address.get("address") == ip
                ):

                    deja_present = True
                    break

            if not deja_present:

                address_lists.add(
                    address=ip,
                    list=list_name,
                    comment=comment
                )

                addresses_added.append(
                    ip
                )

        firewall.add(
            chain="forward",
            action="drop",
            **{
                "src-mac-address": mac,
                "dst-address-list": list_name,
                "comment": comment,
                "place-before": "0"
            }
        )

        return {
            "status": "success",
            "message": (
                "Domaine bloqué "
                "pour cet appareil"
            ),
            "mac": mac,
            "domain": domain,
            "ips": ips,
            "nombre_ips": len(ips),
            "ips_ajoutees": addresses_added,
            "address_list": list_name
        }

    except Exception as e:

        print(
            "ERREUR SITE BLOCK =",
            repr(e)
        )

        return {
            "status": "error",
            "message": (
                f"Erreur MikroTik : {str(e)}"
            ),
            "type": type(e).__name__
        }

    finally:
        api.close()
@app.get("/mikrotik/devices/daily-traffic")
def get_daily_traffic():

    today = datetime.now().date()

    try:

        with engine.connect() as connection:

            rows = connection.execute(
                text("""
                    SELECT
                        mac,
                        ip,
                        hostname,
                        traffic_date,
                        download_bytes,
                        upload_bytes,
                        (
                            download_bytes
                            + upload_bytes
                        ) AS total_bytes,
                        limit_notified,
                        updated_at
                    FROM mikromanager_daily_traffic
                    WHERE traffic_date = :traffic_date
                    ORDER BY total_bytes DESC
                """),
                {
                    "traffic_date": today
                }
            ).mappings().all()

            result = []

            for row in rows:

                total_bytes = int(
                    row["total_bytes"] or 0
                )

                item = dict(row)

                item["download_bytes"] = int(
                    row["download_bytes"] or 0
                )

                item["upload_bytes"] = int(
                    row["upload_bytes"] or 0
                )

                item["total_bytes"] = total_bytes

                item["total_gb"] = round(
                    total_bytes / (1024 ** 3),
                    2
                )

                item["limit_gb"] = 2

                item["limit_exceeded"] = (
                    total_bytes >= DAILY_TRAFFIC_LIMIT
                )

                if item.get("updated_at"):
                    item["updated_at"] = (
                        item["updated_at"].isoformat()
                    )

                result.append(item)

            return {
                "status": "success",
                "date": str(today),
                "limit_bytes": DAILY_TRAFFIC_LIMIT,
                "limit_gb": 2,
                "devices": result
            }

    except Exception as e:

        return {
            "status": "error",
            "message": str(e),
            "type": type(e).__name__
        }

@app.put("/mikrotik/devices/{mac}/site-unblock")
def unblock_site_device(
    mac: str,
    request: ServiceBlockRequest
):

    api = connexion_mikrotik()

    if api is None:

        return {
            "status": "error",
            "message": "Connexion MikroTik échouée"
        }

    try:

        mac = mac.strip().upper()

        domain = normaliser_domaine(
            request.domain
        )

        if not mac:

            return {
                "status": "error",
                "message": "Adresse MAC manquante"
            }

        if not domain:

            return {
                "status": "error",
                "message": "Domaine invalide"
            }

        firewall = api.path(
            "ip",
            "firewall",
            "filter"
        )

        address_lists = api.path(
            "ip",
            "firewall",
            "address-list"
        )

        list_name = nom_address_list(
            mac,
            domain
        )

        comment = (
            f"MikroManager-SITE-{mac}-{domain}"
        )

        rules = list(
            firewall
        )

        rules_deleted = 0

        for rule in rules:

            if rule.get("comment") == comment:

                firewall.remove(
                    rule[".id"]
                )

                rules_deleted += 1

        addresses = list(
            address_lists
        )

        addresses_deleted = 0

        for address in addresses:

            if (
                address.get("list") == list_name
                and address.get("comment") == comment
            ):

                address_lists.remove(
                    address[".id"]
                )

                addresses_deleted += 1

        return {
            "status": "success",
            "message": (
                "Domaine débloqué "
                "pour cet appareil"
            ),
            "mac": mac,
            "domain": domain,
            "rules_deleted": rules_deleted,
            "addresses_deleted": addresses_deleted
        }

    except Exception as e:

        return {
            "status": "error",
            "message": str(e),
            "type": type(e).__name__
        }

    finally:
        api.close()


@app.post("/mikrotik/sites/refresh")
def refresh_sites():

    resultats = actualiser_tous_les_sites()

    if resultats is None:

        return {
            "status": "error",
            "message": "Impossible d'actualiser les sites"
        }

    return {
        "status": "success",
        "message": "Sites actualisés",
        "results": resultats
    }


@app.get("/mikrotik/sites")
def get_blocked_sites():

    api = connexion_mikrotik()

    if api is None:

        return {
            "status": "error",
            "message": "Connexion MikroTik échouée"
        }

    try:

        firewall = api.path(
            "ip",
            "firewall",
            "filter"
        )

        address_lists = api.path(
            "ip",
            "firewall",
            "address-list"
        )

        rules = list(
            firewall
        )

        addresses = list(
            address_lists
        )

        result = []

        for rule in rules:

            comment = rule.get(
                "comment",
                ""
            )

            if not comment.startswith(
                "MikroManager-SITE-"
            ):
                continue

            prefix = "MikroManager-SITE-"

            valeur = comment[
                len(prefix):
            ]

            separateur = valeur.find("-")

            mac = ""

            domain = ""

            if separateur != -1:

                mac = valeur[
                    :separateur
                ]

                domain = valeur[
                    separateur + 1:
                ]

            list_name = nom_address_list(
                mac,
                domain
            )

            ips = []

            for address in addresses:

                if (
                    address.get("list") == list_name
                    and address.get("comment") == comment
                ):

                    ip = address.get(
                        "address"
                    )

                    if ip:
                        ips.append(ip)

            result.append({
                "id": rule.get(".id"),
                "mac": mac,
                "domain": domain,
                "comment": comment,
                "disabled": rule.get(
                    "disabled"
                ),
                "bytes": rule.get(
                    "bytes"
                ),
                "packets": rule.get(
                    "packets"
                ),
                "ips": sorted(
                    set(ips)
                ),
                "nombre_ips": len(
                    set(ips)
                )
            })

        return {
            "status": "success",
            "total": len(result),
            "sites": result
        }

    except Exception as e:

        return {
            "status": "error",
            "message": str(e),
            "type": type(e).__name__
        }

    finally:
        api.close()
        
        
@app.get("/mikrotik/monitoring")
def mikrotik_monitoring():

    api = connexion_mikrotik()

    if api is None:
        return {
            "status": "error",
            "message": "Connexion MikroTik échouée"
        }

    try:

        devices = obtenir_sites_detectes(api)

        total_sites = 0
        total_bytes = 0

        for device in devices:

            total_sites += len(
                device["sites"]
            )

            total_bytes += device["total_bytes"]

        return {
            "status": "success",
            "devices": devices,
            "total_devices": len(devices),
            "total_sites": total_sites,
            "total_bytes": total_bytes,
            "total_formatted": format_octets(
                total_bytes
            )
        }

    except Exception as e:

        print(
            "ERREUR MONITORING =",
            repr(e)
        )

        return {
            "status": "error",
            "message": str(e),
            "type": type(e).__name__
        }

    finally:

        try:
            api.close()
        except:
            pass
