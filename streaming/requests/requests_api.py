from fastapi import FastAPI
from streaming.requests.requests_request import RequestsRequest # type of data API expects in the request body
from streaming.requests.requests_producer import publish_request
from streaming.requests.requests_event import RequestEvent

app = FastAPI()

@app.post("/requests")
def create_requests(request: RequestsRequest):

    event = RequestEvent.create(
        user_id=request.user_id,
        blood_group=request.blood_group,
        units_required=request.units_required,
        latitude=request.latitude,
        longitude=request.longitude
    )

    publish_request(event.to_dict())

    return { # frontend gets this
        "message": "Request event published",
        "data": request
    }