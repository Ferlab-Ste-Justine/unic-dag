-- noinspection SqlNoDataSourceInspectionForFile

-- create mapping table
CREATE TABLE IF NOT EXISTS catalog.mapping
(
    id                         SERIAL PRIMARY KEY,
    value_set_code_id          INTEGER REFERENCES catalog.value_set_code (id) ON DELETE CASCADE NOT NULL,
    original_value             VARCHAR(255)                                                     NOT NULL,
    original_value_description VARCHAR(500),
    last_update                TIMESTAMP                                                        NOT NULL DEFAULT NOW(),
    created_at                 TIMESTAMP                                                        NOT NULL DEFAULT now(),
    UNIQUE (value_set_code_id, original_value)
);