from fastapi import FastAPI, Query
from cachetools import TTLCache
import requests
import os

app = FastAPI()

# Load environment variables
CACHE_TTL = int(os.getenv("CACHE_TTL", "1200"))  # 20 minutes default
BRAPI_TOKEN = os.getenv("BRAPI_TOKEN", "your_token_here")
BRAPI_URL = "https://brapi.dev/api/quote/{ticker}?token={token}&modules=defaultKeyStatistics"

# In-memory cache
cache = TTLCache(maxsize=1000, ttl=CACHE_TTL)

@app.get("/quote/{ticker}")
async def get_quote(ticker: str):
#async def get_quote(ticker: str, privileged: bool = Query(False)):
    ticker = ticker.upper()
    if privileged or ticker not in cache:
        url = BRAPI_URL.format(ticker=ticker, token=BRAPI_TOKEN)
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            cache[ticker] = data
        except Exception as e:
            return {"error": f"Failed to fetch data for {ticker}: {str(e)}"}
    return cache[ticker]

@app.get("/register")
async def get_register(username: str, token: str):
    if username = "xxx" and token = "xxx":
        return {"status": "succeeded"}
    else:
        return {"status": "failed"}

