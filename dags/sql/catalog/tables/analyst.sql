-- noinspection SqlNoDataSourceInspectionForFile

-- create analyst table
CREATE TABLE IF NOT EXISTS catalog.analyst
(
    id          SERIAL PRIMARY KEY,
    last_update TIMESTAMP           NOT NULL,
    name        VARCHAR(255) UNIQUE NOT NULL
);