from dataclasses import dataclass, asdict
from datetime import datetime, timezone
import uuid

@dataclass
class DonationEvent:
    event_id: str
    donor_id: int
    blood_bank_id: int
    blood_group: str
    units_donated: int
    event_time: str

    @staticmethod
    def create(donor_id, blood_bank_id, blood_group, units_donated):
        return DonationEvent(
            event_id=str(uuid.uuid4()),
            donor_id=donor_id,
            blood_bank_id=blood_bank_id,
            blood_group=blood_group,
            units_donated=units_donated,
            event_time=datetime.now(timezone.utc).isoformat()
        )

    def to_dict(self):
        return asdict(self)