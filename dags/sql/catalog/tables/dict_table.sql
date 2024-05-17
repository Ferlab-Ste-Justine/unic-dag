-- noinspection SqlNoDataSourceInspectionForFile

-- create dict_table table
CREATE TABLE IF NOT EXISTS catalog.dict_table
(
    id              SERIAL PRIMARY KEY,
    dictionary_id   INTEGER REFERENCES catalog.dictionary (id) NOT NULL,
    last_update     TIMESTAMP                                  NOT NULL,
    name            VARCHAR(255)                               NOT NULL,
    entity_type     catalog.entity_type_enum                   NOT NULL,
    domain          catalog.domain_type_enum,
    label_en        VARCHAR(500)                               NOT NULL,
    label_fr        VARCHAR(500)                               NOT NULL,
    row_filter      VARCHAR(500),
    to_be_published BOOLEAN                                    NOT NULL
);