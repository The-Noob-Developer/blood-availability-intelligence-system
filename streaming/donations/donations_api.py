from fastapi import FastAPI
from streaming.donations.donations_request import DonationRequest # type of data API expects in the request body
from streaming.donations.donations_producer import publish_donation
from streaming.donations.donations_event import DonationEvent

app = FastAPI()

@app.post("/donations")
def create_donation(donation: DonationRequest):

    event = DonationEvent.create(
        donor_id=donation.donor_id,
        blood_bank_id=donation.blood_bank_id,
        blood_group=donation.blood_group,
        units_donated=donation.units_donated,
    )

    publish_donation(event.to_dict())
    
    # donation.model_dump() : converts a Pydantic model into a standard Python dictionary

    return { # frontend gets this
        "message": "Donation event published",
        "data": donation
    }