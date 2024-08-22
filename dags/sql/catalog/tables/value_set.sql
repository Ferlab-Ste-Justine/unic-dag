-- noinspection SqlNoDataSourceInspectionForFile

-- create value_set table
CREATE TABLE IF NOT EXISTS catalog.value_set
(
    id             SERIAL PRIMARY KEY,
    name           VARCHAR(255) UNIQUE NOT NULL,
    last_update    TIMESTAMP           NOT NULL DEFAULT NOW(),
    created_at     TIMESTAMP           NOT NULL DEFAULT now(),
    description_en VARCHAR(1000),
    description_fr VARCHAR(1000),
    url            VARCHAR(255)
);