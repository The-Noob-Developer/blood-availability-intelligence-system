-- Run this in your Postgres/Neon database to create the auth_user table
-- before using the Google auth service.

CREATE TABLE IF NOT EXISTS auth_user (
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
