import os

from dotenv import load_dotenv
from fastapi import Depends, FastAPI, Header, HTTPException, Request
from starlette.middleware.sessions import SessionMiddleware

from streaming.requests.requests_request import RequestsRequest
from streaming.requests.requests_producer import publish_request
from streaming.requests.requests_event import RequestEvent

load_dotenv()

SESSION_SECRET_KEY = os.getenv("SESSION_SECRET_KEY", "super-secret-session-key")

app = FastAPI()
app.add_middleware(SessionMiddleware, secret_key=SESSION_SECRET_KEY, same_site="lax")


def get_current_user(
    request: Request,
    x_authenticated_user_id: str | None = Header(default=None, alias="X-Authenticated-User-Id"),
):
    user = request.session.get("user")
    if user:
        return user

    if x_authenticated_user_id:
        return {"id": x_authenticated_user_id, "source": "header"}

    raise HTTPException(status_code=401, detail="Not authenticated")


@app.post("/requests")
def create_requests(request: RequestsRequest, user: dict = Depends(get_current_user)):
    user_id = user.get("id")
    if user_id is None:
        raise HTTPException(status_code=401, detail="Authenticated user has no id")

    event = RequestEvent.create(
        user_id=user_id,
        blood_group=request.blood_group,
        units_required=request.units_required,
        city=request.city,
        latitude=request.latitude,
        longitude=request.longitude,
    )

    event_payload = event.to_dict()
    publish_request(event_payload)

    return {
        "message": "Request event published",
        "request_id": event.event_id,
        "status": "PENDING",
        "poll_url": f"/allocations/{event.event_id}",
        "data": request,
        "authenticated_user": user,
    }
