import os
import re
import time
from datetime import datetime
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

STATUS_STYLE = {
    "PENDING": {"icon": "⏳", "label": "Searching for a match", "color": "#f59e0b"},
    "ALLOCATED": {"icon": "✅", "label": "Blood bank found", "color": "#16a34a"},
    "TIMEOUT": {"icon": "⚠️", "label": "Taking longer than expected", "color": "#dc2626"},
    "ERROR": {"icon": "❌", "label": "Something went wrong", "color": "#dc2626"},
    "UNKNOWN": {"icon": "🔄", "label": "Checking status", "color": "#64748b"},
}


st.set_page_config(
    page_title="Blood Availability Intelligence System - HRG",
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
            st.session_state.auth_message = None

        # Clear the auth params from the URL so a rerun doesn't keep
        # re-processing the same query string.
        st.query_params.clear()
    elif auth_status == "logged_out":
        st.session_state.authenticated_user = None
        st.session_state.auth_message = "Signed out"
        st.query_params.clear()


def normalize_base_url(value: str) -> str:
    return value.rstrip("/")


def derive_display_name(email: str) -> str:
    """Builds a friendly display name out of an email address, since the
    auth provider only gives us an email + id, not a real name field."""
    if not email:
        return "Guest"
    local_part = email.split("@")[0]
    cleaned = re.sub(r"[._\-]+", " ", local_part).strip()
    cleaned = re.sub(r"\d+", "", cleaned).strip()
    if not cleaned:
        cleaned = local_part
    return cleaned.title()


def derive_initials(display_name: str) -> str:
    parts = [p for p in display_name.split() if p]
    if not parts:
        return "?"
    if len(parts) == 1:
        return parts[0][:2].upper()
    return (parts[0][0] + parts[1][0]).upper()


def format_datetime(iso_str: Optional[str]) -> str:
    if not iso_str:
        return "-"
    try:
        cleaned = iso_str.replace("Z", "+00:00")
        dt = datetime.fromisoformat(cleaned)
        return dt.strftime("%b %d, %Y · %I:%M %p")
    except (ValueError, TypeError):
        return iso_str


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


def render_status_card(status: str, extra_html: str = "") -> None:
    style = STATUS_STYLE.get(status, STATUS_STYLE["UNKNOWN"])
    st.markdown(
        f"""
        <div style="
            border-left: 4px solid {style['color']};
            background: rgba(148, 163, 184, 0.08);
            border-radius: 0.75rem;
            padding: 1rem 1.2rem;
            margin-bottom: 0.75rem;
        ">
            <div style="font-size: 1.4rem; line-height: 1;">{style['icon']}&nbsp; <span style="font-size:1.05rem; font-weight:600; vertical-align:middle;">{style['label']}</span></div>
            {extra_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_allocation_result(result: Dict[str, Any]) -> None:
    status = result.get("status", "UNKNOWN")

    if status == "PENDING":
        render_status_card(
            "PENDING",
            "<div style='opacity:0.75; margin-top:0.35rem;'>We're matching your request with the nearest blood bank. This page updates automatically.</div>",
        )
        return

    if status in ("ERROR", "TIMEOUT"):
        detail = result.get("error", "Please try tracking again in a moment.")
        render_status_card(
            status,
            f"<div style='opacity:0.75; margin-top:0.35rem;'>{detail}</div>",
        )
        return

    if status == "ALLOCATED":
        bank_name = result.get("blood_bank_name") or f"Blood Bank #{result.get('blood_bank_id', '-')}"
        blood_group = result.get("blood_group", "-")
        units = result.get("units_allocated", "-")
        distance = result.get("distance_km")
        fulfillment_time = format_datetime(result.get("fulfillment_time"))

        render_status_card("ALLOCATED")

        st.markdown(
            f"""
            <div style="
                border: 1px solid rgba(148, 163, 184, 0.25);
                border-radius: 1rem;
                padding: 1.25rem 1.4rem;
                background: rgba(248, 250, 252, 0.9);
                margin-bottom: 0.75rem;
            ">
                <div style="font-size: 1.25rem; font-weight: 700; margin-bottom: 0.9rem;">🏥 {bank_name}</div>
                <div style="display:flex; flex-wrap:wrap; gap:1.75rem;">
                    <div>
                        <div style="font-size:0.8rem; opacity:0.6;">Blood Group</div>
                        <div style="font-size:1.15rem; font-weight:600;">🩸 {blood_group}</div>
                    </div>
                    <div>
                        <div style="font-size:0.8rem; opacity:0.6;">Units Allocated</div>
                        <div style="font-size:1.15rem; font-weight:600;">{units}</div>
                    </div>
                    {"<div><div style='font-size:0.8rem; opacity:0.6;'>Distance</div><div style='font-size:1.15rem; font-weight:600;'>📍 " + f"{distance:.1f} km" + "</div></div>" if distance is not None else ""}
                    <div>
                        <div style="font-size:0.8rem; opacity:0.6;">Confirmed</div>
                        <div style="font-size:1.15rem; font-weight:600;">🕒 {fulfillment_time}</div>
                    </div>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        ref = result.get("request_id", "")
        if ref:
            st.caption(f"Reference: {ref[-8:].upper()}")
        return

    render_status_card("UNKNOWN")
    with st.expander("Details"):
        st.json(result)


def render_sidebar_tracking_card() -> None:
    request_id = st.session_state.get("active_request_id")
    if not request_id:
        st.caption("No active request being tracked yet.")
        return

    status = st.session_state.get("active_request_status") or "PENDING"
    style = STATUS_STYLE.get(status, STATUS_STYLE["UNKNOWN"])
    short_ref = request_id[-8:].upper()

    st.markdown(
        f"""
        <div style="
            border-left: 4px solid {style['color']};
            background: rgba(148, 163, 184, 0.08);
            border-radius: 0.6rem;
            padding: 0.7rem 0.9rem;
        ">
            <div style="font-size: 1.3rem;">{style['icon']}</div>
            <div style="font-weight: 600; margin-top: 0.15rem;">{style['label']}</div>
            <div style="font-size: 0.75rem; opacity: 0.65; margin-top: 0.2rem;">Ref: {short_ref}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


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

authenticated_user = st.session_state.get("authenticated_user")
display_name = derive_display_name((authenticated_user or {}).get("email", ""))
initials = derive_initials(display_name)

# --- Top user greeting bar ---
if authenticated_user:
    st.markdown(
        f"""
        <div style="display:flex; align-items:center; gap:0.85rem; margin-bottom:1.1rem;">
            <div style="
                width: 46px; height: 46px; border-radius: 50%;
                background: linear-gradient(135deg, #1e3a8a, #0f766e);
                color: white; display:flex; align-items:center; justify-content:center;
                font-weight:700; font-size:1.05rem; flex-shrink:0;
            ">{initials}</div>
            <div>
                <div style="font-weight:700; font-size:1.1rem;">Welcome, {display_name}</div>
                <div style="font-size:0.85rem; opacity:0.6;">{authenticated_user.get('email', '')}</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
else:
    st.info("👋 Sign in with Google from the sidebar to request or donate blood.")

with st.sidebar:
    st.header("Authentication")
    if authenticated_user:
        st.success(f"Signed in as {display_name}")
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
    st.subheader("📡 Tracking")
    render_sidebar_tracking_card()
    if st.session_state.get("active_request_id"):
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
    if authenticated_user:
        st.caption(f"Requesting as **{display_name}**")
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
                st.success("🎉 Your request has been submitted! We're finding the nearest match for you.")
                st.caption(f"Reference: {response['request_id'][-8:].upper()}")
            except requests.RequestException as exc:
                error_detail = None
                if exc.response is not None:
                    try:
                        error_detail = exc.response.json().get("detail", exc.response.text)
                    except ValueError:
                        error_detail = exc.response.text

                st.error(f"Failed to submit request: {error_detail or exc}")

with tab_donate:
    st.subheader("Record a donation")
    if authenticated_user:
        st.caption(f"Donating as **{display_name}**")
    with st.form("donation_form", clear_on_submit=False):
        col1, col2 = st.columns(2)
        with col1:
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
                st.success(f"🙏 Thank you, {display_name}! Your donation has been recorded.")
            except requests.RequestException as exc:
                st.error(f"Failed to submit donation: {exc}")

with tab_track:
    st.subheader("Track a request")
    manual_request_id = st.text_input(
        "Request reference or ID",
        value=st.session_state.get("active_request_id", ""),
        placeholder="Paste your request reference here",
    )
    track_clicked = st.button("Track Request", use_container_width=True)

    if track_clicked and manual_request_id and manual_request_id.strip():
        ensure_tracking_state(manual_request_id.strip())

    current_request_id = st.session_state.get("active_request_id")
    if current_request_id:
        st.caption(f"Tracking reference: {current_request_id[-8:].upper()}")
        allocation_result = st.session_state.get("active_allocation_result")
        if allocation_result:
            render_allocation_result(allocation_result)
    else:
        st.info("Submit a request or enter a request reference to start tracking.")

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