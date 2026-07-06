from pydantic import BaseModel

class RequestsRequest(BaseModel):
    user_id: int
    blood_group: str
    units_required: int
    latitude: float
    longitude: float