from pydantic import BaseModel

class RequestsRequest(BaseModel):
    user_id: int
    blood_group: str
    city: str
    units_required: int
    latitude: float
    longitude: float