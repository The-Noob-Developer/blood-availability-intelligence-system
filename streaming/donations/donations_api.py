from fastapi import FastAPI
from streaming.donations.donations_request import DonationRequest
from streaming.donations.donations_producer import publish_donation

app = FastAPI()

@app.post("/donations")
def create_donation(donation: DonationRequest):

    publish_donation(donation.model_dump())

    return {
        "message": "Donation event published",
        "data": donation
    }