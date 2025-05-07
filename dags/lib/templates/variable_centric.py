import json

VARIABLE_CENTRIC = """
{
  "index_patterns": [
    "variable_centric_*"
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
        "var_id": {
          "type": "integer"
        },
        "var_label_en": {
          "type": "keyword",
          "normalizer" : "custom_normalizer"
        },
        "var_label_fr": {
          "type": "keyword",
          "normalizer" : "custom_normalizer"
        },
        "var_last_update": {
          "type": "date"
        },
        "var_created_at": {
          "type": "date"
        },
        "var_derivation_algorithm": {
          "type": "keyword",
          "index": false
        },
        "var_name": {
          "type": "keyword",
          "normalizer" : "custom_normalizer"
        },
        "var_notes": {
          "type": "keyword",
          "index": false
        },
        "var_path": {
          "type": "keyword"
        },
        "var_status": {
          "type": "keyword"
        },
        "var_value_type": {
          "type": "keyword"
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
        "table": {
          "properties": {
            "tab_id": {
              "type": "integer"
            },
            "tab_label_fr": {
              "type": "keyword"
            },
            "tab_label_en": {
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
            "tab_name": {
              "type": "keyword"
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
              "type": "keyword"
            },
            "rs_name": {
              "type": "keyword",
              "index": false
            },
            "published": {
              "type": "boolean",
              "index": false
            }
          }
        },
        "var_from_variables": {
          "type": "nested",
          "properties": {
            "var_id": {
              "type": "integer"
            },
            "var_name": {
              "type": "keyword"
            },
            "published": {
              "type": "boolean",
              "index": false
            },
            "resource": {
              "type": "nested",
              "properties": {
                "rs_id": {
                  "type": "integer"
                },
                "rs_name": {
                  "type": "keyword"
                },
                "rs_code": {
                  "type": "keyword"
                }
              }
            },
            "table": {
              "type": "nested",
              "properties": {
                "tab_id": {
                  "type": "integer"
                },
                "tab_name": {
                  "type": "keyword"
                },
                "tab_label_en": {
                  "type": "keyword"
                },
                "tab_label_fr": {
                  "type": "keyword"
                }
              }
            }
          }
        },
        "value_set": {
          "properties": {
            "vs_id": {
              "type": "integer"
            },
            "vs_name": {
              "type": "keyword"
            },
            "vs_description_en": {
              "type": "keyword",
              "index": false
            },
            "vs_description_fr": {
              "type": "keyword",
              "index": false
            },
            "values": {
              "type": "nested",
              "properties": {
                "vsval_code": {
                  "type": "keyword"
                },
                "vsval_label_en": {
                  "type": "keyword",
                  "index": false
                },
                "vsval_label_fr": {
                  "type": "keyword",
                  "index": false
                }
              }
            }
          }
        }
      }
    }
  }
}
"""

# Load JSON into a Python dictionary
variable_centric_template = json.loads(VARIABLE_CENTRIC)