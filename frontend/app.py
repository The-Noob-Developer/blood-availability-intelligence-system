import os
import time
from typing import Any, Dict, Optional

import requests
import streamlit as st
from browser_geolocation import browser_geolocation


DONATION_API_BASE_URL = os.getenv("DONATION_API_BASE_URL", "http://127.0.0.1:8000")
REQUEST_API_BASE_URL = os.getenv("REQUEST_API_BASE_URL", "http://127.0.0.1:8002")
ALLOCATION_API_BASE_URL = os.getenv("ALLOCATION_API_BASE_URL", "http://127.0.0.1:8001")
AUTH_API_BASE_URL = os.getenv("AUTH_API_BASE_URL", "http://127.0.0.1:8003")
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "2"))

SUPPORTED_BLOOD_GROUPS = [
    "A+",
    "A-",
    "B+",
    "B-",
    "AB+",
    "AB-",
    "O+",
    "O-",
]


st.set_page_config(
    page_title="Blood Availability Intelligence System",
    layout="wide",
)


def init_request_location_state() -> None:
    st.session_state.setdefault("request_latitude", 11.675442)
    st.session_state.setdefault("request_longitude", 92.747338)
    st.session_state.setdefault("request_location_source", "manual")


def init_auth_state() -> None:
    st.session_state.setdefault("authenticated_user", None)
    st.session_state.setdefault("auth_message", None)


def sync_auth_state_from_query_params() -> None:
    """Reads the auth callback query params (?auth=success&email=...&user_id=...)
    and stores them in session state.

    Note: user_id is a UUID string (Snowflake's auth_user.id is UUID_STRING()-backed),
    not an integer, so it is stored and used as-is — no int() coercion anywhere.
    """
    params = st.query_params
    auth_status = params.get("auth")
    if auth_status == "success":
        email = params.get("email", "")
        user_id = params.get("user_id", "")
        if isinstance(email, list):
            email = email[0] if email else ""
        if isinstance(user_id, list):
            user_id = user_id[0] if user_id else ""

        if email or user_id:
            st.session_state.authenticated_user = {
                "email": email,
                "id": user_id if user_id else None,
            }
            st.session_state.auth_message = f"Signed in as {email or user_id}"

        # Clear the auth params from the URL so a rerun doesn't keep
        # re-processing the same query string.
        st.query_params.clear()
    elif auth_status == "logged_out":
        st.session_state.authenticated_user = None
        st.session_state.auth_message = "Signed out"
        st.query_params.clear()


def normalize_base_url(value: str) -> str:
    return value.rstrip("/")


def render_same_tab_link_button(label: str, url: str) -> None:
    """Render a button-styled link that navigates in the SAME browser tab.

    st.link_button always opens a new tab, which spins up a brand new
    Streamlit session. That new session receives the OAuth callback and
    gets marked as authenticated, while the original tab (where the user
    actually submits requests/donations) never learns about it. A plain
    anchor with target="_self" keeps the whole redirect round-trip inside
    the same session.
    """
    st.markdown(
        f"""
        <a href="{url}" target="_self" style="text-decoration: none;">
            <div style="
                display: inline-block;
                width: 100%;
                box-sizing: border-box;
                text-align: center;
                padding: 0.5rem 1rem;
                border-radius: 0.5rem;
                border: 1px solid rgba(49, 51, 63, 0.2);
                background-color: #ffffff;
                color: #262730;
                font-weight: 500;
                cursor: pointer;
            ">
                {label}
            </div>
        </a>
        """,
        unsafe_allow_html=True,
    )


def request_json(
    method: str,
    url: str,
    payload: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    response = requests.request(method, url, json=payload, headers=headers, timeout=15)
    response.raise_for_status()
    return response.json()


def fetch_allocation(request_id: str) -> Dict[str, Any]:
    url = f"{normalize_base_url(ALLOCATION_API_BASE_URL)}/allocations/{request_id}"
    response = requests.get(url, timeout=15)
    response.raise_for_status()
    return response.json()


def submit_request_form(payload: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{normalize_base_url(REQUEST_API_BASE_URL)}/requests"
    headers = {}
    auth_user = st.session_state.get("authenticated_user") or {}
    auth_user_id = auth_user.get("id")
    if auth_user_id is not None:
        headers["X-Authenticated-User-Id"] = str(auth_user_id)
    return request_json("POST", url, payload, headers=headers)


def submit_donation_form(payload: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{normalize_base_url(DONATION_API_BASE_URL)}/donations"
    headers = {}
    auth_user = st.session_state.get("authenticated_user") or {}
    auth_user_id = auth_user.get("id")
    if auth_user_id is not None:
        # Although the backend may not use this header, we send it for consistency
        # with the request submission flow.
        headers["X-Authenticated-User-Id"] = str(auth_user_id)
    return request_json("POST", url, payload, headers=headers)


def render_allocation_result(result: Dict[str, Any]) -> None:
    status = result.get("status", "UNKNOWN")
    if status == "PENDING":
        st.info("Request is still pending. The page will keep checking for an allocation.")
        return

    if status == "ALLOCATED":
        cols = st.columns(4)
        cols[0].metric("Blood Bank ID", result.get("blood_bank_id", "-"))
        cols[1].metric("Blood Group", result.get("blood_group", "-"))
        cols[2].metric("Units Allocated", result.get("units_allocated", "-"))
        cols[3].metric("Request ID", result.get("request_id", "-"))

        st.success("Allocation found.")
        allocation_details = {
            "fulfillment_time": result.get("fulfillment_time"),
            "blood_bank_id": result.get("blood_bank_id"),
            "blood_bank_name": result.get("blood_bank_name"),
        }
        if result.get("distance_km") is not None:
            allocation_details["distance_km"] = result.get("distance_km")
        st.write(allocation_details)
        return

    st.warning(f"Unexpected status: {status}")
    st.json(result)


def ensure_tracking_state(request_id: str) -> None:
    st.session_state.active_request_id = request_id
    st.session_state.tracking_enabled = True
    st.session_state.poll_count = 0


def live_poll_active_request() -> None:
    request_id = st.session_state.get("active_request_id")
    tracking_enabled = st.session_state.get("tracking_enabled", False)
    if not request_id or not tracking_enabled:
        return

    poll_count = st.session_state.get("poll_count", 0)
    if poll_count >= 30:
        st.session_state.tracking_enabled = False
        st.session_state.active_request_status = "TIMEOUT"
        return

    try:
        st.session_state.poll_count = poll_count + 1
        result = fetch_allocation(request_id)
        st.session_state.active_allocation_result = result
        st.session_state.active_request_status = result.get("status", "UNKNOWN")
    except requests.RequestException as exc:
        st.session_state.active_allocation_result = {"status": "ERROR", "error": str(exc)}
        st.session_state.active_request_status = "ERROR"
        return

    if st.session_state.active_request_status == "PENDING":
        time.sleep(POLL_INTERVAL_SECONDS)
        st.rerun()


st.markdown(
    """
    <style>
        .hero {
            padding: 1.5rem 1.75rem;
            border-radius: 1.25rem;
            background: linear-gradient(135deg, #0f172a 0%, #1e3a8a 55%, #0f766e 100%);
            color: white;
            margin-bottom: 1.25rem;
        }
        .hero h1 {
            margin: 0;
            font-size: 2rem;
        }
        .hero p {
            margin: 0.5rem 0 0;
            opacity: 0.9;
        }
        .card {
            padding: 1rem 1.1rem;
            border-radius: 1rem;
            border: 1px solid rgba(148, 163, 184, 0.22);
            background: rgba(248, 250, 252, 0.94);
        }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div class="hero">
        <h1>Blood Availability Intelligence System</h1>
        <p>Request blood, donate blood, and track allocation status in one place.</p>
    </div>
    """,
    unsafe_allow_html=True,
)

init_request_location_state()
init_auth_state()
sync_auth_state_from_query_params()

with st.sidebar:
    st.header("Authentication")
    if st.session_state.get("authenticated_user"):
        user = st.session_state.authenticated_user
        st.success(f"Signed in as {user.get('email') or user.get('id')}")
        if st.button("Sign out"):
            st.session_state.authenticated_user = None
            st.session_state.auth_message = "Signed out"
            st.rerun()
        render_same_tab_link_button(
            "Logout on server", f"{normalize_base_url(AUTH_API_BASE_URL)}/auth/logout"
        )
    else:
        st.info("Sign in with Google before submitting a request.")
        render_same_tab_link_button(
            "Sign in with Google", f"{normalize_base_url(AUTH_API_BASE_URL)}/auth/login"
        )

    if st.session_state.get("auth_message"):
        st.caption(st.session_state.auth_message)

    st.divider()
    st.header("API Settings")
    REQUEST_API_BASE_URL = st.text_input("Request API Base URL", value=REQUEST_API_BASE_URL)
    ALLOCATION_API_BASE_URL = st.text_input("Allocation API Base URL", value=ALLOCATION_API_BASE_URL)
    DONATION_API_BASE_URL = st.text_input("Donation API Base URL", value=DONATION_API_BASE_URL)
    POLL_INTERVAL_SECONDS = st.number_input("Poll interval (seconds)", min_value=1, max_value=30, value=POLL_INTERVAL_SECONDS)

    st.divider()
    st.subheader("Tracking")
    active_request_id = st.session_state.get("active_request_id")
    st.write(active_request_id or "No active request")
    if st.button("Stop tracking", use_container_width=True):
        st.session_state.tracking_enabled = False
        st.session_state.active_request_id = None
        st.session_state.active_allocation_result = None
        st.session_state.active_request_status = None
        st.session_state.poll_count = 0
        st.rerun()

tab_request, tab_donate, tab_track = st.tabs(["Request Blood", "Donate Blood", "Track Request"])

with tab_request:
    st.subheader("Create a blood request")
    st.caption("Use your browser location to auto-fill latitude and longitude.")
    location_result = browser_geolocation(key="request_location")
    if location_result and location_result.get("latitude") is not None and location_result.get("longitude") is not None:
        st.session_state.request_latitude = float(location_result["latitude"])
        st.session_state.request_longitude = float(location_result["longitude"])
        st.session_state.request_location_source = "browser"
        st.success(
            f"Location captured: {st.session_state.request_latitude:.6f}, {st.session_state.request_longitude:.6f}"
        )
    elif location_result and location_result.get("error"):
        st.error(f"Unable to fetch location: {location_result['error']}")

    with st.form("request_form", clear_on_submit=False):
        col1, col2 = st.columns(2)
        with col1:
            auth_user = st.session_state.get("authenticated_user") or {}
            default_user_id = auth_user.get("id") or ""
            # User ID is a UUID string tied to the authenticated Google account,
            # not a number, so this is a disabled text field rather than a
            # number_input. It's derived from the session, not hand-entered.
            request_user_id = st.text_input(
                "User ID", value=str(default_user_id), disabled=True
            )
            blood_group = st.selectbox("Blood Group", SUPPORTED_BLOOD_GROUPS, index=6)
            city = st.text_input("City", value="Port Blair")
        with col2:
            units_required = st.number_input("Units Required", min_value=1, step=1, value=1)
            latitude = st.number_input(
                "Latitude",
                value=float(st.session_state.request_latitude),
                format="%.6f",
            )
            longitude = st.number_input(
                "Longitude",
                value=float(st.session_state.request_longitude),
                format="%.6f",
            )

        st.caption(f"Location source: {st.session_state.request_location_source}")

        submitted = st.form_submit_button("Submit Request", use_container_width=True)

    if submitted:
        auth_user = st.session_state.get("authenticated_user") or {}

        # Add logging to inspect the authentication state upon submission
        with st.expander("Debug: Authentication State on Submit"):
            st.write("`st.session_state.authenticated_user`:")
            st.json(st.session_state.get("authenticated_user", "Not found in session state"))
            st.write("`auth_user` variable used for check:")
            st.json(auth_user)
            st.write("Value of `auth_user.get('id')`:")
            st.write(auth_user.get("id"))

        if not auth_user.get("id"):
            st.warning("Please sign in with Google first so the request is linked to your account.")
        else:
            try:
                # user_id is a UUID string — no int() coercion.
                user_id = auth_user.get("id")
                payload = {
                    "user_id": user_id,
                    "blood_group": blood_group,
                    "city": city,
                    "units_required": int(units_required),
                    "latitude": float(latitude),
                    "longitude": float(longitude),
                }
                response = submit_request_form(payload)
                ensure_tracking_state(response["request_id"])
                st.success("Request submitted successfully.")
                st.json(response)
            except requests.RequestException as exc:
                st.error(f"Failed to submit request: {exc}")

with tab_donate:
    st.subheader("Record a donation")
    with st.form("donation_form", clear_on_submit=False):
        col1, col2 = st.columns(2)
        with col1:
            auth_user = st.session_state.get("authenticated_user") or {}
            default_user_id = auth_user.get("id") or ""
            # Donor ID is the same UUID as the authenticated user's ID.
            donor_id = st.text_input(
                "Donor ID", value=str(default_user_id), key="donor_id", disabled=True
            )
            blood_bank_id = st.number_input("Blood Bank ID", min_value=1, step=1, value=101)
        with col2:
            donation_group = st.selectbox("Blood Group", SUPPORTED_BLOOD_GROUPS, index=6, key="donation_group")
            units_donated = st.number_input("Units Donated", min_value=1, step=1, value=1)

        donation_submitted = st.form_submit_button("Submit Donation", use_container_width=True)

    if donation_submitted:
        auth_user = st.session_state.get("authenticated_user") or {}
        if not auth_user.get("id"):
            st.warning("Please sign in with Google first so the donation is linked to your account.")
        else:
            try:
                payload = {
                    "donor_id": auth_user.get("id"),  # UUID string, no int() coercion
                    "blood_bank_id": int(blood_bank_id),
                    "blood_group": donation_group,
                    "units_donated": int(units_donated),
                }
                response = submit_donation_form(payload)
                st.success("Donation submitted successfully.")
                st.json(response)
            except requests.RequestException as exc:
                st.error(f"Failed to submit donation: {exc}")

with tab_track:
    st.subheader("Track a request")
    manual_request_id = st.text_input("Request ID", value=st.session_state.get("active_request_id", ""))
    track_clicked = st.button("Track Request", use_container_width=True)

    if track_clicked and manual_request_id and manual_request_id.strip():
        ensure_tracking_state(manual_request_id.strip())

    current_request_id = st.session_state.get("active_request_id")
    if current_request_id:
        st.caption(f"Polling request: {current_request_id}")
        allocation_result = st.session_state.get("active_allocation_result")
        if allocation_result:
            render_allocation_result(allocation_result)
    else:
        st.info("Submit a request or enter a request ID to start tracking.")

if st.session_state.get("active_request_id") and st.session_state.get("tracking_enabled", False):
    st.divider()
    st.subheader("Live Allocation Status")
    try:
        live_poll_active_request()
        live_result = st.session_state.get("active_allocation_result")
        if live_result is None:
            live_result = fetch_allocation(st.session_state.active_request_id)
            st.session_state.active_allocation_result = live_result
        render_allocation_result(live_result)
    except requests.RequestException as exc:
        st.error(f"Unable to poll allocation status: {exc}")