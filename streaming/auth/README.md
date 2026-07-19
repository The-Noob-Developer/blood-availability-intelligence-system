# Auth Service

This folder contains the Gmail OAuth authentication service for the Blood Availability Intelligence System.

## What this provides

- `/auth/login` — redirects the user to Google for authentication
- `/auth/callback` — handles Google callback, creates or updates the user record, and stores the user session
- `/auth/me` — returns the authenticated user from session
- `/auth/logout` — clears the user session

## Required environment variables

- `GOOGLE_CLIENT_ID`
- `GOOGLE_CLIENT_SECRET`
- `SESSION_SECRET_KEY`
- `FRONTEND_URL` (optional, defaults to `http://127.0.0.1:8501`)

## Required Python packages

Install in your backend virtual environment:

```bash
pip install authlib python-dotenv
```

The project already uses FastAPI and Starlette.

## Database setup

Run this SQL in the same Postgres database used by the app:

```sql
CREATE TABLE auth_user (
    id SERIAL PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    full_name TEXT,
    given_name TEXT,
    family_name TEXT,
    picture TEXT,
    locale TEXT,
    google_sub TEXT NOT NULL UNIQUE,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_login_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

### Optional useful DB changes

If you want to attach donations and requests to authenticated users, add a `user_id` foreign key to those tables:

```sql
ALTER TABLE donation_event ADD COLUMN user_id INTEGER REFERENCES auth_user(id);
ALTER TABLE blood_requests ADD COLUMN user_id INTEGER REFERENCES auth_user(id);
```

Then update the donation and request APIs to include `user_id` from the authenticated user.
