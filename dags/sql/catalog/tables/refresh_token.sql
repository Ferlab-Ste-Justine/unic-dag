CREATE TABLE IF NOT EXISTS catalog.refresh_token
(
    id         SERIAL PRIMARY KEY,
    user_id    INT REFERENCES catalog.user (id) NOT NULL,
    token      TEXT                             NOT NULL,
    created_at TIMESTAMP                        NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMP                        NOT NULL
);