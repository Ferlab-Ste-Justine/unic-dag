resource_centric_template = {
  "index_patterns": [
    "resource_centric_*"
  ],
  "template": {
    "settings": {
      "index": {
        "number_of_shards": "3",
        "mapping": {
          "nested_objects": {
            "limit": "45000"
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
    "mappings": {
      "properties": {
        "rs_id": {
          "type": "integer"
        },
        "rs_system_collection_starting_year": {
          "type": "integer"
        },
        "rs_type": {
          "type": "keyword"
        },
        "rs_dict_current_version": {
          "type": "keyword"
        },
        "rs_system_database_type": {
          "type": "keyword"
        },
        "rs_project_creation_date": {
          "type": "date"
        },
        "rs_project_completion_date": {
          "type": "date"
        },
        "rs_last_update": {
          "type": "date"
        },
        "rs_project_approval_date": {
          "type": "date"
        },
        "rs_project_approved": {
          "type": "boolean"
        },
        "rs_project_folder": {
          "type": "keyword"
        },
        "rs_project_status": {
          "type": "keyword"
        },
        "rs_project_erb_id": {
          "type": "keyword",
          "normalizer" : "custom_normalizer"
        },
        "rs_project_pi": {
          "type": "keyword",
          "normalizer" : "custom_normalizer"
        },
        "rs_description_en": {
          "type": "keyword",
          "normalizer" : "custom_normalizer"
        },
        "rs_description_fr": {
          "type": "keyword",
          "normalizer" : "custom_normalizer"
        },
        "rs_title": {
          "type": "keyword",
          "normalizer" : "custom_normalizer"
        },
        "rs_name": {
          "type": "keyword",
          "normalizer" : "custom_normalizer"
        },
        "rs_code": {
          "type": "keyword",
          "normalizer" : "custom_normalizer"
        },
        "rs_is_project": {
          "type": "boolean"
        },
        "stat_etl": {
          "properties": {
            "project_count": {
              "type": "long"
            },
            "domain_count": {
              "type": "long"
            },
            "source_system_count": {
              "type": "long"
            },
            "variable_count": {
              "type": "long"
            },
            "table_count": {
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
                },
                "stat_etl": {
                  "properties": {
                    "variable_count": {
                      "type": "long"
                    }
                  }
                }
              }
            }
          }
        },
        "tables": {
          "type": "nested",
          "properties": {
            "tab_id": {
              "type": "integer"
            },
            "tab_name": {
              "type": "keyword"
            },
            "tab_label_fr": {
              "type": "keyword"
            },
            "tab_label_en": {
              "type": "keyword"
            }
          }
        },
        "rs_analyst": {
          "properties": {
            "analyst_id": {
              "type": "integer"
            },
            "analyst_name": {
              "type": "keyword"
            }
          }
        }
      }
    }
  }
}