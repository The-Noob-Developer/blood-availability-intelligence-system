from dataclasses import dataclass, asdict
from datetime import datetime
import uuid


@dataclass
class DonationEvent:

    donor_id: int
    blood_bank_id: int
    blood_group: str
    units: int

    def to_dict(self):
        return {
            "event_id": str(uuid.uuid4()),
            "event_type": "DONATION_COMPLETED",
            "timestamp": datetime.utcnow().isoformat(),
            **asdict(self)
        }