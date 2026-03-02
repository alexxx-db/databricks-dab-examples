from pydantic import BaseModel, Field


class TaxiTrip(BaseModel):
    id: int
    tpep_pickup_datetime: str
    tpep_dropoff_datetime: str
    trip_distance: float = Field(ge=0)
    fare_amount: float = Field(ge=0)
    pickup_zip: int
    dropoff_zip: int
