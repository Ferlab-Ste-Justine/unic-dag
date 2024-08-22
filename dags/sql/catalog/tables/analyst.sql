-- noinspection SqlNoDataSourceInspectionForFile

-- create analyst table
CREATE TABLE IF NOT EXISTS catalog.analyst
(
    id          SERIAL PRIMARY KEY,
    last_update TIMESTAMP           NOT NULL DEFAULT NOW(),
    created_at  TIMESTAMP           NOT NULL DEFAULT now(),
    name        VARCHAR(255) UNIQUE NOT NULL
);