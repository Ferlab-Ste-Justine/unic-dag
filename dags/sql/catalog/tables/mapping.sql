-- noinspection SqlNoDataSourceInspectionForFile

-- create mapping table
CREATE TABLE IF NOT EXISTS catalog.mapping
(
    id                SERIAL PRIMARY KEY,
    value_set_code_id INTEGER REFERENCES value_set_code (id) NOT NULL,
    original_value    VARCHAR(255)                           NOT NULL,
    last_update       TIMESTAMP                              NOT NULL
);