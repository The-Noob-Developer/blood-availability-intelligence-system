from dataclasses import dataclass, asdict
from datetime import datetime, timezone
import uuid

@dataclass
class RequestEvent:
    event_id: str
    user_id: int
    blood_group: str
    units_required: int
    latitude: float
    longitude: float
    event_time: str

    @staticmethod
    def create(user_id, blood_group, units_required, latitude, longitude):
        return RequestEvent(
            event_id=str(uuid.uuid4()),
            user_id=user_id,
            blood_group=blood_group,
            units_required=units_required,
            latitude=latitude,
            longitude=longitude,
            event_time=datetime.now(timezone.utc).isoformat()
        )

    def to_dict(self):
        return asdict(self)