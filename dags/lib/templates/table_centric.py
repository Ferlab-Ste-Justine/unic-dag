table_centric_template = {
  "index_patterns": [
    "table_centric_*"
  ],
  "template": {
    "settings": {
      "index": {
        "number_of_shards": "3",
        "mapping": {
          "nested_objects": {
            "limit": "30000"
          }
        },
        "analysis": {
          "normalizer": {
            "custom_normalizer": {
              "type": "custom",
              "char_filter": [],
              "filter": [
                "lowercase",
                "asciifolding"
              ]
            }
          }
        }
      }
    },
    "mappings" : {
      "properties": {
        "tab_id": {
          "type": "integer"
        },
        "tab_created_at": {
          "type": "date"
        },
        "tab_domain": {
          "type": "keyword"
        },
        "tab_domain_en": {
          "type": "keyword",
          "normalizer" : "custom_normalizer"
        },
        "tab_domain_fr": {
          "type": "keyword",
          "normalizer" : "custom_normalizer"
        },
        "tab_entity_type": {
          "type": "keyword"
        },
        "tab_label_en": {
          "type": "keyword",
          "normalizer" : "custom_normalizer"
        },
        "tab_label_fr": {
          "type": "keyword",
          "normalizer" : "custom_normalizer"
        },
        "tab_last_update": {
          "type": "date"
        },
        "tab_name": {
          "type": "keyword",
          "normalizer" : "custom_normalizer"
        },
        "tab_row_filter": {
          "type": "keyword",
          "index": False
        },
        "resource": {
          "properties": {
            "rs_id": {
              "type": "integer"
            },
            "rs_code": {
              "type": "keyword"
            },
            "rs_is_project": {
              "type": "boolean"
            },
            "rs_description_en": {
              "type": "keyword"
            },
            "rs_description_fr": {
              "type": "keyword"
            },
            "rs_type": {
              "type": "keyword"
            },
            "rs_name": {
              "type": "keyword"
            },
            "rs_title": {
              "type": "keyword"
            }
          }
        },
        "stat_etl": {
          "properties": {
            "variable_count": {
              "type": "long"
            }
          }
        },
        "variables": {
          "type": "nested",
          "properties": {
            "var_id": {
              "type": "integer"
            },
            "var_name": {
              "type": "keyword"
            },
            "var_label_fr": {
              "type": "keyword"
            },
            "var_label_en": {
              "type": "keyword"
            },
            "var_from_variables": {
              "type": "nested",
              "properties": {
                "var_id": {
                  "type": "integer"
                },
                "var_name": {
                  "type": "keyword",
                  "index": False
                },
                "published": {
                  "type": "boolean",
                  "index": False
                }
              }
            },
            "var_from_source_systems": {
              "type": "nested",
              "properties": {
                "rs_id": {
                  "type": "integer"
                },
                "rs_code": {
                  "type": "keyword",
                  "index": False
                },
                "rs_name": {
                  "type": "keyword",
                  "index": False
                },
                "published": {
                  "type": "boolean",
                  "index": False
                }
              }
            }
          }
        }
      }
    }
  }
}
