import asyncio
import logging
import os
from typing import Dict, Optional

import requests
from fastapi import Depends, HTTPException, Request, Response, status

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# The URL for the central authentication service.
# In a real deployment, this would come from a config service or environment variable.
AUTH_SERVICE_URL = os.getenv("AUTH_SERVICE_URL", "http://localhost:8003")


async def get_current_user(request: Request) -> Dict:
    """
    A reusable FastAPI dependency to check for an authenticated user.

    This function is called by other services to protect their endpoints. It works by:
    1. Extracting the session cookie from the incoming request.
    2. Forwarding that cookie to the central `/auth/me` endpoint.
    3. If a user is returned, the request is considered authenticated.
    4. If not, an HTTP 401 Unauthorized error is raised.

    Args:
        request: The incoming FastAPI request.

    Returns:
        The user dictionary if authenticated.

    Raises:
        HTTPException: If the user is not authenticated.
    """
    logger.info("Attempting to get current user from session.")
    session_cookie = request.cookies.get("session")
    if not session_cookie:
        logger.warning("No session cookie found in request. User is not authenticated.")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated")

    auth_url = f"{AUTH_SERVICE_URL}/auth/me"
    logger.info(f"Forwarding auth check to: {auth_url}")

    try:
        # Use the synchronous `requests` library in a separate thread to avoid
        # blocking the async event loop and to prevent library conflicts.
        response = await asyncio.to_thread(
            requests.get,
            auth_url,
            cookies={"session": session_cookie},
            timeout=5,
        )
        response.raise_for_status()
        user = response.json()
    except requests.RequestException as e:
        logger.error(f"Auth service request failed: {e}", exc_info=True)
        # Log response body if available and it's an error
        if e.response is not None:
            logger.error(f"Auth service response status: {e.response.status_code}")
            logger.error(f"Auth service response body: {e.response.text}")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid session")
    except ValueError:
        logger.error("Failed to decode JSON from auth service response.", exc_info=True)
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid session")

    if not user:
        logger.warning("Auth service returned an empty user object. Invalidating session.")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid session")

    logger.info(f"Successfully authenticated user: {user.get('email')}")
    return user