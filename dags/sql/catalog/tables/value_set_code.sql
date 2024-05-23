-- noinspection SqlNoDataSourceInspectionForFile

-- create value_set_code table
CREATE TABLE IF NOT EXISTS catalog.value_set_code
(
    id           SERIAL PRIMARY KEY,
    value_set_id INTEGER REFERENCES catalog.value_set (id) NOT NULL,
    last_update  TIMESTAMP                                 NOT NULL DEFAULT NOW(),
    code         VARCHAR(50)                               NOT NULL,
    label_fr     VARCHAR(255)                              NOT NULL,
    label_en     VARCHAR(255)                              NOT NULL,
    UNIQUE (value_set_id, code)
);