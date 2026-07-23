"""
Curated CSCMED DAG
"""
from datetime import timedelta
from typing import Dict, List

import pendulum
from airflow import DAG

from lib.config import CONFIG_FILE, JAR, SPARK_FAILURE_MSG, DEFAULT_ARGS, DEFAULT_PARAMS, LOCAL_TZ
from lib.slack import Slack
from tasks import create_tasks
from timetables import IntervalTimetable

DOC = """
# Curated CscMed DAG

ETL curated et anonymized pour toutes les tables CscMed à l'exception des tables jobs et jobs_sections.

### Description
Ce DAG traite les tables chargées lors de la seconde batch de chargement de CscMed par Talend :
- **curated_cscmed_quickform** (zone rouge, curated)
- Toutes les tables **anonymized_cscmed_*** (zone jaune, anonymized), soit :
  ado, aid, all, anes, atoe, aud, car, chi, chusj, cir, cpa, cra, crme, ctc, demographic, den, der,
  dev, devcirene, devd, deve, dia, div, dou, end, esej, fkp, gas, ge, hem, imm, inh, mep, min, mmo,
  nch, neo, nep, neu, nut, obg, obs, orl, ort, pal, ped, phy, pla, psc, pso, psy, pul, quickform,
  rams, rhu, rneu, rped, rphy, rsat, scol, sjm, soc, sto, tel, uro

Les tables jobs et jobs_sections sont traitées par le DAG `curated_cscmed_jobs`.

### Tests QA
Les tests QA anonymized (`pre_tests` / `post_tests`) sont découpés par première lettre de table
(`anonymized_cscmed_a*`, `anonymized_cscmed_c*`, ...) plutot qu'exécutés en un seul job.
Les lettres sont dérivées de la liste des datasets ci-dessous : toute
nouvelle table est automatiquement couverte par le test de sa lettre.

### Horaire
* __Date de début__ - 9 avril 2026
* __Jour et heure__ - Jeudi, 20h heure de Montréal
* __Intervalle__ - Chaque 4 semaines
"""

ANON_PREFIX = "anonymized_cscmed_"

ANON_DATASETS = [
    {"dataset_id": "anonymized_cscmed_ado*"       , "cluster_type": "small" , "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_aid*"       , "cluster_type": "small" , "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_all*"       , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_anes*"      , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_atoe*"      , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_aud*"       , "cluster_type": "small" , "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_car*"       , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_chi*"       , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_chusj*"     , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_cir*"       , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_cpa*"       , "cluster_type": "small" , "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_cra*"       , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_crme*"      , "cluster_type": "small" , "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_ctc*"       , "cluster_type": "small" , "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_demographic", "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_den*"       , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_der*"       , "cluster_type": "small" , "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_dev_*"      , "cluster_type": "small" , "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_devcirene*" , "cluster_type": "small" , "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_devd*"      , "cluster_type": "small" , "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_deve*"      , "cluster_type": "small" , "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_dia*"       , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_div*"       , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_dou*"       , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_end*"       , "cluster_type": "small" , "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_esej*"      , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_fkp*"       , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_gas*"       , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_ge*"        , "cluster_type": "large" , "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_hem*"       , "cluster_type": "large" , "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_imm*"       , "cluster_type": "small" , "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_inh*"       , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_mep*"       , "cluster_type": "small" , "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_min*"       , "cluster_type": "small" , "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_mmo*"       , "cluster_type": "small" , "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_nch*"       , "cluster_type": "small" , "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_neo*"       , "cluster_type": "small" , "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_nep*"       , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_neu*"       , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_nut*"       , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_obg*"       , "cluster_type": "small" , "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_obs*"       , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_orl*"       , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_ort*"       , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_pal*"       , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_ped*"       , "cluster_type": "large" , "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_phy*"       , "cluster_type": "small" , "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_pla*"       , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_psc*"       , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_pso*"       , "cluster_type": "small" , "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_psy*"       , "cluster_type": "small" , "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_pul*"       , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_quickform"  , "cluster_type": "small" , "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_rams*"      , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_rhu*"       , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_rneu*"      , "cluster_type": "small" , "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_rped*"      , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_rphy*"      , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_rsat*"      , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_scol*"      , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_sjm*"       , "cluster_type": "large" , "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_soc*"       , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_sto*"       , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_tel*"       , "cluster_type": "small" , "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_uro*"       , "cluster_type": "small" , "run_type": "default", "pass_date": False, "dependencies": []}
]


def anon_letters() -> List[str]:
    """First letter of each anonymized table name (after the ANON_PREFIX), sorted and deduplicated."""
    return sorted({dataset["dataset_id"][len(ANON_PREFIX)] for dataset in ANON_DATASETS})


def split_by_letter(test_name: str, cluster_type: str = "small") -> List[Dict]:
    return [
        {"name": test_name, "destinations": [f"{ANON_PREFIX}{letter}*"],
         "cluster_type": cluster_type, "suffix": letter}
        for letter in anon_letters()
    ]


dag_config = {
    "steps": [
        {
            "destination_zone": "red",
            "destination_subzone": "curated",
            "main_class": "bio.ferlab.ui.etl.red.curated.Main",
            "multiple_main_methods": True,
            "pre_tests": [{"name": "greater_or_equal_partition_counts", "destinations": ["curated_cscmed_quickform"], "cluster_type": "small"}],
            "datasets": [
                {"dataset_id": "curated_cscmed_quickform", "cluster_type": "small", "run_type": "default", "pass_date": False, "dependencies": []}
            ],
            "optimize": [],
            "post_tests": []
        },
        {
            "destination_zone": "yellow",
            "destination_subzone": "anonymized",
            "main_class": "bio.ferlab.ui.etl.yellow.anonymized.Main",
            "multiple_main_methods": False,
            "pre_tests": split_by_letter("greater_or_equal_partition_counts"),
            "datasets": ANON_DATASETS,
            "optimize": [],
            "post_tests": split_by_letter("lower_or_equal_null_counts") + split_by_letter("equal_counts")
        }
    ]
}

args = DEFAULT_ARGS.copy()

dag = DAG(
    dag_id="curated_cscmed",
    doc_md=DOC,
    start_date=pendulum.datetime(2026, 4, 9, 20, tz=LOCAL_TZ),
    schedule=IntervalTimetable(interval=timedelta(weeks=4)),
    params=DEFAULT_PARAMS,
    dagrun_timeout=timedelta(hours=12),
    default_args=args,
    concurrency=3,
    catchup=False,
    max_active_runs=1,
    tags=["curated"],
    is_paused_upon_creation=True,
    on_failure_callback=Slack.notify_dag_failure
)

with dag:
    create_tasks(
        dag=dag,
        dag_config=dag_config,
        config_file=CONFIG_FILE,
        jar=JAR,
        resource="cscmed",
        spark_failure_msg=SPARK_FAILURE_MSG
    )
