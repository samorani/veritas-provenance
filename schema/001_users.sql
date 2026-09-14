-- 001_users.sql: accounts for the search app.
--
-- Emails are unique regardless of case: the unique index is on lower(email).
-- The app stores the email as entered and looks it up with lower(email).
-- password_hash holds a werkzeug.security.generate_password_hash string.

CREATE TABLE app_user (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    email         TEXT        NOT NULL,
    password_hash TEXT        NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX app_user_email_lower_key ON app_user (lower(email));
