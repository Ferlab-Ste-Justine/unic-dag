-- noinspection SqlNoDataSourceInspectionForFile

-- create dict_table table
CREATE TABLE IF NOT EXISTS catalog.dict_table
(
    id              SERIAL PRIMARY KEY,
    resource_id     INTEGER REFERENCES catalog.resource (id) ON DELETE CASCADE NOT NULL,
    last_update     TIMESTAMP                                                  NOT NULL DEFAULT NOW(),
    created_at      TIMESTAMP                                                  NOT NULL DEFAULT now(),
    name            VARCHAR(255)                                               NOT NULL,
    entity_type     catalog.entity_type_enum,
    domain          catalog.domain_type_enum,
    label_en        VARCHAR(500),
    label_fr        VARCHAR(500),
    row_filter      VARCHAR(500),
    to_be_published BOOLEAN                                                    NOT NULL,
    UNIQUE (resource_id, name)
);