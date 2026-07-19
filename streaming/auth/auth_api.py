import os
from typing import Dict
import logging
from authlib.integrations.starlette_client import OAuth
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from starlette.middleware.sessions import SessionMiddleware
from starlette.responses import RedirectResponse, JSONResponse
import snowflake.connector

from streaming.auth import auth_db
from streaming.auth.auth_models import UserCreate

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

@app.on_event("startup")
async def startup_event():
    """Checks the database connection when the application starts."""
    logger.info("Checking database connection on startup...")
    try:
        # Use a `with` statement to ensure the connection is closed.
        with auth_db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            if cursor.fetchone() == (1,):
                logger.info("Database connection successful.")
            else:
                raise RuntimeError("Database connection check failed unexpectedly.")
    except snowflake.connector.Error as e:
        logger.error(f"Database connection failed: {e}", exc_info=True)
        raise RuntimeError("Could not connect to the database. Please check connection settings and ensure the database is running.") from e

# Ensure the session secret key is set, as it's required for signing session cookies.
session_secret_key = os.getenv("SESSION_SECRET_KEY")
if not session_secret_key:
    raise RuntimeError("SESSION_SECRET_KEY is not set in the environment. This is required for session management.")

# This is the key fix: Add SessionMiddleware with a secret key and same_site='lax'.
# The 'lax' setting is crucial for OAuth redirects to work correctly.
app.add_middleware(
    SessionMiddleware,
    secret_key=session_secret_key,
    same_site="lax",
    https_only=False,  # Set to True in production if using HTTPS
)

oauth = OAuth()


oauth.register(
    name="google",
    client_id=os.getenv("GOOGLE_CLIENT_ID"),
    client_secret=os.getenv("GOOGLE_CLIENT_SECRET"),
    # Let authlib fetch the metadata itself. This is more robust and avoids the startup error.
    server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",
    client_kwargs={"scope": "openid email profile"},
)


@app.get("/auth/login")
async def login(request: Request):
    """Redirects the user to Google for authentication."""
    logger.info("Received request for /auth/login")
    # Using an explicit redirect_uri from an environment variable is more robust
    # and avoids mismatches when running behind a proxy or on different ports.
    # Ensure this URI is registered in your Google Cloud Console.
    redirect_uri = os.getenv("AUTH_REDIRECT_URI", str(request.url_for("auth_callback")))
    logger.info(f"Using redirect_uri: {redirect_uri}")
    try:
        response = await oauth.google.authorize_redirect(request, redirect_uri)
        logger.info("Successfully generated redirect response to Google.")
        return response
    except Exception as e:
        logger.error(f"Error during authorize_redirect: {e}", exc_info=True)
        return JSONResponse(
            status_code=500, content={"error": "Failed to generate Google auth redirect."}
        )


@app.get("/auth/callback")
async def auth_callback(request: Request):
    """Handles the Google callback, creates/updates the user, and stores the session."""
    try:
        token = await oauth.google.authorize_access_token(request)
    except Exception as e:
        # This is where the "mismatching_state" error would be caught.
        return JSONResponse(
            status_code=400,
            content={"error": "OAuth callback failed", "detail": str(e)},
        )

    user_info = token.get("userinfo")
    if not user_info:
        return JSONResponse(
            status_code=400, content={"error": "Failed to get user info from token"}
        )

    user_create = UserCreate(
        email=user_info["email"],
        full_name=user_info.get("name"),
        given_name=user_info.get("given_name"),
        family_name=user_info.get("family_name"),
        picture=user_info.get("picture"),
        locale=user_info.get("locale"),
        google_sub=user_info["sub"],
    )

    user = auth_db.create_or_update_user(user_create)

    # Store essential user info in the session
    request.session["user"] = user

    frontend_url = os.getenv("FRONTEND_URL", "http://127.0.0.1:8501")
    # This is the key fix: Pass user info back to the frontend via query parameters.
    # The frontend app will use these parameters to set its own session state.
    redirect_url = f"{frontend_url}?auth=success&email={user['email']}&user_id={user['id']}"

    return RedirectResponse(url=redirect_url)


@app.get("/auth/me")
async def get_current_user(request: Request) -> Dict:
    """Returns the authenticated user from the session."""
    user = request.session.get("user")
    if not user:
        return {}
    return user


@app.get("/auth/logout")
async def logout(request: Request):
    """Clears the user session."""
    request.session.clear()
    frontend_url = os.getenv("FRONTEND_URL", "http://127.0.0.1:8501")
    return RedirectResponse(url=frontend_url)


# To run this for testing:
# uvicorn streaming.auth.auth_api:app --reload

# Make sure you have these environment variables set in your .env file:
# GOOGLE_CLIENT_ID=...
# GOOGLE_CLIENT_SECRET=...
# SESSION_SECRET_KEY=... (a long random string)
# FRONTEND_URL=http://localhost:8501 (or your streamlit app's address)