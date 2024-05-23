-- noinspection SqlNoDataSourceInspectionForFile

-- create dictionary table
CREATE TABLE IF NOT EXISTS catalog.dictionary
(
    id              SERIAL PRIMARY KEY,
    resource_id     INTEGER REFERENCES catalog.resource (id) UNIQUE NOT NULL,
    last_update     TIMESTAMP                                       NOT NULL DEFAULT NOW(),
    current_version INTEGER                                         NOT NULL,
    to_be_published BOOLEAN                                         NOT NULL
);