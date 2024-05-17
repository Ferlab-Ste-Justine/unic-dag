-- noinspection SqlNoDataSourceInspectionForFile

-- create variable table
CREATE TABLE IF NOT EXISTS catalog.variable
(
    id                   SERIAL PRIMARY KEY,
    table_id             INTEGER REFERENCES dict_table (id) NOT NULL,
    last_update          TIMESTAMP                          NOT NULL,
    name                 VARCHAR(255)                       NOT NULL,
    path                 VARCHAR(255) UNIQUE                NOT NULL,
    value_type           catalog.value_type_enum            NOT NULL,
    label_fr             VARCHAR(255)                       NOT NULL,
    label_en             VARCHAR(255)                       NOT NULL,
    value_set_id         INTEGER REFERENCES value_set (id),
    from_variable_id     INTEGER[],
    derivation_algorithm VARCHAR(500),
    notes                VARCHAR(255),
    variable_status      catalog.status_enum                NOT NULL,
    rolling_version      catalog.rolling_version_enum       NOT NULL,
    to_be_published      BOOLEAN                            NOT NULL
);