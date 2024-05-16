-- noinspection SqlNoDataSourceInspectionForFile

CREATE INDEX IF NOT EXISTS idx_resource_name ON catalog.resource (name);
CREATE INDEX IF NOT EXISTS idx_resource_code ON catalog.resource (code);
CREATE INDEX IF NOT EXISTS idx_analyst_name ON catalog.analyst (name);
CREATE INDEX IF NOT EXISTS idx_value_set_name ON catalog.value_set (name);
CREATE INDEX IF NOT EXISTS idx_dict_table_name ON catalog.dict_table (name);
CREATE INDEX IF NOT EXISTS idx_dict_table_domain ON catalog.dict_table (domain);
CREATE INDEX IF NOT EXISTS idx_variable_name ON catalog.variable (name);
CREATE INDEX IF NOT EXISTS idx_value_set_code_code ON catalog.value_set_code (code);