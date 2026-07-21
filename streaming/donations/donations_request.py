from pydantic import BaseModel

class DonationRequest(BaseModel):
    donor_id: str
    blood_bank_id: int
    blood_group: str
    units_donated: int