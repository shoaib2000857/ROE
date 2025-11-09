from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from functools import lru_cache
from datetime import datetime
import pandas as pd

DATA_PATH = "q-fastapi-timeseries-cache.csv"

df = pd.read_csv(DATA_PATH)
df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_localize(None)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ✅ We maintain our OWN cache-status dictionary
cache_status = {}

@lru_cache(maxsize=512)
def compute_stats(location, sensor, start_date, end_date):
    d = df.copy()

    if location:
        d = d[d["location"] == location]

    if sensor:
        d = d[d["sensor"] == sensor]

    if start_date:
        d = d[d["timestamp"] >= pd.to_datetime(start_date)]

    if end_date:
        d = d[d["timestamp"] <= pd.to_datetime(end_date)]

    if d.empty:
        return {"count": 0, "avg": None, "min": None, "max": None}

    return {
        "count": int(d["value"].count()),
        "avg": float(d["value"].mean()),
        "min": float(d["value"].min()),
        "max": float(d["value"].max()),
    }


@app.get("/stats")
async def get_stats(
    response: Response,
    location: str = None,
    sensor: str = None,
    start_date: str = None,
    end_date: str = None,
):

    # ✅ Build cache key (must match LRU key exactly)
    cache_key = (location, sensor, start_date, end_date)

    # ✅ Determine HIT / MISS manually
    if cache_key in cache_status:
        response.headers["X-Cache"] = "HIT"
    else:
        response.headers["X-Cache"] = "MISS"
        cache_status[cache_key] = True  # mark as cached

    stats = compute_stats(location, sensor, start_date, end_date)

    return {"stats": stats}
