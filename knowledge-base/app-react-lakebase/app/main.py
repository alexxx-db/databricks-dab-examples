import logging
from typing import List

from database import db_connection
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from models import TaxiTrip

logger = logging.getLogger(__name__)

app = FastAPI()

app_frontend = StaticFiles(directory="frontend/dist", html=True)
app_api = FastAPI()

app.mount("/api", app_api)
app.mount("/", app_frontend)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_taxi_trips_data() -> List[TaxiTrip]:
    query = f"""
        SELECT id, tpep_pickup_datetime, tpep_dropoff_datetime, trip_distance,
               fare_amount, pickup_zip, dropoff_zip
        FROM {db_connection.postgres_schema}.{db_connection.postgres_table}
        ORDER BY tpep_pickup_datetime DESC
        LIMIT 100
    """

    try:
        with db_connection.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()
    except Exception as e:
        logger.error(f"Database query failed: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch taxi trip data")

    return [
        TaxiTrip(
            id=row[0],
            tpep_pickup_datetime=row[1].isoformat(),
            tpep_dropoff_datetime=row[2].isoformat(),
            trip_distance=row[3],
            fare_amount=row[4],
            pickup_zip=row[5],
            dropoff_zip=row[6],
        )
        for row in rows
    ]


@app_api.get("/taxi-trips", response_model=List[TaxiTrip])
def get_taxi_trips():
    return get_taxi_trips_data()
