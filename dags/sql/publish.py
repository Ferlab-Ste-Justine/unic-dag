
def resource_query(resource_code: str) -> str:
    """
    Query to get resource information for a given resource code.
    """
    return f"""
        SELECT * FROM catalog.resource 
        WHERE code='{resource_code}'
        AND r.to_be_published = true
    """

def dict_table_query(resource_code: str) -> str:
    """
    Query to get tables for a given resource code.
    """
    return f"""
        SELECT t.*
        FROM catalog.dict_table t
        INNER JOIN catalog.resource r
        ON t.resource_id = r.id
        WHERE r.code='{resource_code}'
        AND r.to_be_published = true
    """

def variable_query(resource_code: str) -> str:
    """
    Query to get variables for a given resource code.
    """
    return f"""
        WITH filt_dict_table AS (
            SELECT t.id AS filt_table_id 
            FROM catalog.dict_table t
            INNER JOIN catalog.resource r
            ON t.resource_id = r.id
            WHERE r.code='{resource_code}'
            AND r.to_be_published = true
        )
        SELECT v.* 
        FROM catalog.variable v
        INNER JOIN filt_dict_table fdt
        ON fdt.filt_table_id = v.table_id
    """

def value_set_query(resource_code: str) -> str:
    """
    Query to get value sets for a given resource code.
    """
    return f"""
        WITH filt_dict_table AS (
            SELECT t.id AS filt_table_id 
            FROM catalog.dict_table t
            INNER JOIN catalog.resource r
            ON t.resource_id = r.id
            where r.code='{resource_code}'
            AND r.to_be_published = true
        ),
        filt_variable AS (
            SELECT v.value_set_id AS filt_value_set_id
            FROM catalog.variable v
            INNER JOIN filt_dict_table fdt
            ON fdt.filt_table_id = v.table_id
        )
        SELECT vs.* 
        FROM catalog.value_set vs
        INNER JOIN filt_variable fv
        ON fv.filt_value_set_id = vs.id
    """

def value_set_code_query(resource_code: str) -> str:
    """
    Query to get value set codes for a given resource code.
    """
    return f"""
        WITH filt_dict_table AS (
            SELECT t.id AS filt_table_id 
            FROM catalog.dict_table t
            INNER JOIN catalog.resource r
            ON t.resource_id = r.id
            WHERE r.code='{resource_code}'
            AND r.to_be_published = true
        ),
        filt_variable as (
            SELECT v.value_set_id AS filt_value_set_id
            FROM catalog.variable v
            INNER JOIN filt_dict_table fdt
            ON fdt.filt_table_id = v.table_id
        )
        SELECT vsc.* 
        FROM catalog.value_set_code vsc
        INNER JOIN filt_variable fv
        ON fv.filt_value_set_id = vsc.value_set_id
    """

def mapping_query(resource_code: str) -> str:
    """
    Query to get mappings for a given resource code.
    """
    return f"""
        WITH filt_dict_table AS (
            SELECT t.id AS filt_table_id 
            FROM catalog.dict_table t
            INNER JOIN catalog.resource r
            ON t.resource_id = r.id
            WHERE r.code='{resource_code}'
            AND r.to_be_published = true
        ),
        filt_variable AS (
            SELECT v.value_set_id AS filt_value_set_id
            FROM catalog.variable v
            INNER JOIN filt_dict_table fdt
            ON fdt.filt_table_id = v.table_id
        ),
        filt_value_set_code AS (
            SELECT vsc.id AS filt_value_set_code_id
            FROM catalog.value_set_code vsc
            INNER JOIN filt_variable fv
            ON fv.filt_value_set_id = vsc.value_set_id
        )
        SELECT m.* 
        FROM catalog.mapping m
        INNER JOIN filt_value_set_code fvsc
        ON fvsc.filt_value_set_code_id = m.value_set_code_id
    """

def update_dict_current_version_query(resource_code: str, dict_version: str) -> None:
    """
    Query to update 'dict_current_version' for a given resource code.
    """
    return f"""
        UPDATE catalog.resource
        SET dict_current_version = '{dict_version}'
        WHERE recourse_id = '{resource_code}';
    """
