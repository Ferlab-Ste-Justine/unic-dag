-- noinspection SqlNoDataSourceInspectionForFile

-- create analyst table
CREATE TABLE IF NOT EXISTS catalog.analyst
(
    id          SERIAL PRIMARY KEY,
    last_update TIMESTAMP           NOT NULL DEFAULT NOW(),
    name        VARCHAR(255) UNIQUE NOT NULL
);