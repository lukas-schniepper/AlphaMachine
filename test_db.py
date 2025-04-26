# test_db.py

import os
import socket
import psycopg2

# 1) URL laden
url = os.getenv("DATABASE_URL")
print("▶️ Using DATABASE_URL:", url)

# 2) Host extrahieren
#    Wir splitten zwischen '@' und ':' 
host = url.split("@")[1].split(":")[0]
print("▶️ Testing host:", host)

# 3) DNS-Auflösung
try:
    ip = socket.gethostbyname(host)
    print(f"✅ DNS ok: {host} → {ip}")
except Exception as e:
    print(f"❌ DNS failed for {host}: {e}")

# 4) Port-Check
sock = socket.socket()
sock.settimeout(5)
try:
    sock.connect((host, 5432))
    print("✅ Port 5432 reachable")
except Exception as e:
    print(f"❌ Port check failed: {e}")
finally:
    sock.close()

# 5) Versuch, sich mit psycopg2 zu verbinden
try:
    conn = psycopg2.connect(url)
    print("✅ DB connection successful")
    conn.close()
except Exception as e:
    print(f"❌ DB connect error: {e}")
