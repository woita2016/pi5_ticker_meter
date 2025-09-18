import requests
import os
import psycopg2

from fastapi import FastAPI, Query
from cachetools import TTLCache
from psycopg2 import pool

app = FastAPI()

# Load environment variables
USER_CACHE_TTL = int(os.getenv("USER_CACHE_TTL", "86400")) 
CACHE_TTL = int(os.getenv("CACHE_TTL", "1200")) 
DB_URL = os.getenv("DB_URL", "postgres://<username>:<password>@<host>:<port>/<database>") 
BRAPI_TOKEN = os.getenv("BRAPI_TOKEN", "your_token_here")
BRAPI_URL = "https://brapi.dev/api/quote/{ticker}?token={token}&modules=defaultKeyStatistics"

# In-memory cache
cache = TTLCache(maxsize=1000, ttl=CACHE_TTL)
user_cache = TTLCache(maxsize=1000, ttl=USER_CACHE_TTL)

# Database connection pool
db_pool = pool.SimpleConnectionPool(
    minconn=1,
    maxconn=10,
    dsn=DB_URL
)

def get_user(input_username, input_token, force_verify):
    if input_username not in user_cache or force_verify:
        conn = db_pool.getconn()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT privileged
                    FROM users
                    WHERE username = %s AND token = %s AND status = %s
                    LIMIT 1;
                """, (input_username, input_token, 'active'))
                result = cursor.fetchone()
                user_cache[input_username] = result
                return result
        except Exception as e:
            print(f"DB error: {e}")
            return None
        finally:
            db_pool.putconn(conn)
    else:
        return user_cache[input_username]

def initialize_users_table():
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cursor:
            # Create table if it doesn't exist
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    username TEXT PRIMARY KEY,
                    token TEXT NOT NULL,
                    status TEXT NOT NULL,
                    privileged TEXT NOT NULL
                );
            """)

            # Check if admin user exists
            cursor.execute("""
                SELECT 1 FROM users WHERE username = %s;
            """, ("admin",))
            exists = cursor.fetchone()

            # Insert default admin if not present
            if not exists:
                cursor.execute("""
                    INSERT INTO users (username, token, status, privileged)
                    VALUES (%s, %s, %s, %s);
                """, ("admin", "password", "active", "yes"))

            conn.commit()
    except Exception as e:
        print(f"Initialization error: {e}")
    finally:
        db_pool.putconn(conn)

initialize_users_table()

#async def get_quote(ticker: str, privileged: bool = Query(False)):
@app.get("/quote/{ticker}")
async def get_quote(ticker: str, username: str, token: str):
    ticker = ticker.upper()
    result = get_user(username, token, False)
    if result is None:
        return {"error": f"Failed to fetch data for {ticker}: {str(e)}"}
    else:
        if result == "yes" or ticker not in cache:
            url = BRAPI_URL.format(ticker=ticker, token=BRAPI_TOKEN)
            try:
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                data = response.json()
                cache[ticker] = data
            except Exception as e:
                return {"error": f"Failed to fetch data for {ticker}: {str(e)}"}
        return cache[ticker]

@app.get("/user_check")
async def get_register(username: str, token: str):
    result = get_user(username, token, True)
    if result is None:
        return {"status": "failed"}
    else:
        return {"status": result}
