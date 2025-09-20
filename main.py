import requests
import os
import psycopg2
import psycopg2.extras

from fastapi import FastAPI, Query, Body
from cachetools import TTLCache
from psycopg2 import pool
from pydantic import BaseModel

###########################################################################################################################

app = FastAPI()

###########################################################################################################################

# Load environment variables
USER_CACHE_TTL = int(os.getenv("USER_CACHE_TTL", "86400")) 
CACHE_TTL = int(os.getenv("CACHE_TTL", "1200")) 
DB_URL = os.getenv("DB_URL", "postgres://<username>:<password>@<host>:<port>/<database>") 
BRAPI_TOKEN = os.getenv("BRAPI_TOKEN", "your_token_here")
BRAPI_URL = os.getenv("BRAPI_URL", "https://brapi.dev/api/quote/{ticker}?token={token}&modules=defaultKeyStatistics")

# In-memory cache
cache = TTLCache(maxsize=1000, ttl=CACHE_TTL)
user_cache = TTLCache(maxsize=1000, ttl=USER_CACHE_TTL)

# Database connection pool
db_pool = pool.SimpleConnectionPool(
    minconn=1,
    maxconn=10,
    dsn=DB_URL
)

###########################################################################################################################

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
                if result is None:
                    return None
                else:
                    user_cache[input_username] = result[0]
                    return result[0]
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

###########################################################################################################################

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

###########################################################################################################################

@app.get("/user_check")
async def user_check(username: str, token: str):
    result = get_user(username, token, True)
    if result is None:
        return {"status": "failed"}
    else:
        return {"status": result}

###########################################################################################################################

class AdminUserUpdatePayload(BaseModel):
    target_username: str
    token: str | None = None
    status: str | None = None
    privileged: str | None = None

@app.put("/update_user")
async def update_user(username: str, token: str, payload: AdminUserUpdatePayload = Body(...)):
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cursor:
            # Verify admin identity
            cursor.execute("""
                SELECT 1 FROM users
                WHERE username = %s AND token = %s AND status = 'active'
                LIMIT 1;
            """, (username, token))
            if cursor.fetchone() is None or username != "admin":
                return {"status": "unauthorized"}

            # Build update query
            fields = []
            values = []

            if payload.token is not None:
                fields.append("token = %s")
                values.append(payload.token)
            if payload.status is not None:
                fields.append("status = %s")
                values.append(payload.status)
            if payload.privileged is not None:
                fields.append("privileged = %s")
                values.append(payload.privileged)

            if not fields:
                return {"status": "failed", "reason": "no fields to update"}

            values.append(payload.target_username)

            query = f"""
                UPDATE users
                SET {', '.join(fields)}
                WHERE username = %s
            """

            cursor.execute(query, values)
            conn.commit()
            if cursor.rowcount == 0:
                return {"status": "failed", "reason": "target user not found"}
            return {"status": "succeeded"}
    except Exception as e:
        print(f"DB error: {e}")
        return {"status": "failed", "reason": str(e)}
    finally:
        db_pool.putconn(conn)

###########################################################################################################################

class AddUserPayload(BaseModel):
    target_username: str
    token: str
    status: str
    privileged: str

@app.post("/add_user")
async def add_user(username: str, token: str, payload: AddUserPayload = Body(...)):
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cursor:
            # Verify admin identity
            cursor.execute("""
                SELECT 1 FROM users
                WHERE username = %s AND token = %s AND status = 'active'
                LIMIT 1;
            """, (username, token))
            if cursor.fetchone() is None or username != "admin":
                return {"status": "unauthorized"}

            # Check if target user already exists
            cursor.execute("""
                SELECT 1 FROM users WHERE username = %s LIMIT 1;
            """, (payload.target_username,))
            if cursor.fetchone():
                return {"status": "failed", "reason": "user already exists"}

            # Insert new user
            cursor.execute("""
                INSERT INTO users (username, token, status, privileged)
                VALUES (%s, %s, %s, %s);
            """, (
                payload.target_username,
                payload.token,
                payload.status,
                payload.privileged
            ))
            conn.commit()
            return {"status": "succeeded"}
    except Exception as e:
        print(f"DB error: {e}")
        return {"status": "failed", "reason": str(e)}
    finally:
        db_pool.putconn(conn)

###########################################################################################################################

class TokenUpdatePayload(BaseModel):
    new_token: str

@app.put("/update_user_token")
async def update_user_token(username: str, token: str, payload: TokenUpdatePayload = Body(...)):
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                UPDATE users
                SET token = %s
                WHERE username = %s AND token = %s
            """, (payload.new_token, username, token))
            conn.commit()
            if cursor.rowcount == 0:
                return {"status": "failed", "reason": "user not found or token mismatch"}
            return {"status": "succeeded"}
    except Exception as e:
        print(f"DB error: {e}")
        return {"status": "failed", "reason": str(e)}
    finally:
        db_pool.putconn(conn)

###########################################################################################################################

@app.get("/user_list")
async def user_list(username: str, token: str, target_username: str | None = None):
    conn = db_pool.getconn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            # Verify admin token
            cursor.execute("""
                SELECT 1 FROM users
                WHERE username = %s AND token = %s AND status = 'active'
                LIMIT 1;
            """, (username, token))
            if cursor.fetchone() is None or username != "admin":
                return {"status": "unauthorized"}

            # Fetch filtered or full user list
            if target_username:
                cursor.execute("""
                    SELECT username, token, status, privileged
                    FROM users
                    WHERE username = %s;
                """, (target_username,))
            else:
                cursor.execute("""
                    SELECT username, token, status, privileged
                    FROM users;
                """)

            rows = cursor.fetchall()
            users = [dict(row) for row in rows]
            return {"status": "succeeded", "users": users}
    except Exception as e:
        print(f"DB error: {e}")
        return {"status": "failed", "reason": str(e)}
    finally:
        db_pool.putconn(conn)
