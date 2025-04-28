# test_db.py
from sqlalchemy import create_engine
import os

#url = os.getenv("DATABASE_URL")
url ="postgresql://postgres.rmjvbadnwrgduojlasas:Y0yZz3J12XKKyH9N@aws-0-eu-central-1.pooler.supabase.com:6543/postgres?sslmode=require"
#DATABASE_URL ="postgresql://postgres.rmjvbadnwrgduojlasas:Y0yZz3J12XKKyH9N@aws-0-eu-central-1.pooler.supabase.com:6543/postgres"
print("▶️ Using DATABASE_URL:", url)
engine = create_engine(url, connect_args={"sslmode": "require"})
try:
    with engine.connect() as conn:
        print("✅ Engine.connect() OK")
except Exception as e:
    print("❌ Engine.connect() failed:", e)
