"""
Curated CSCMED DAG
"""
from datetime import timedelta

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
- Les tables **curated_cscmed_*** (zone rouge, curated), soit : quickform,
  gas_notemedicalebreve_v, hem_bilan_inr_entete_p, hem_thrombophilie_entete_p,
  scol_autresparametresradiologiques_p
- Toutes les tables **anonymized_cscmed_*** (zone jaune, anonymized), soit :
  ado, aid, all, anes, atoe, aud, car, chi, chusj, cir, cpa, cra, crme, ctc, demographic, den, der,
  dev, devcirene, devd, deve, dia, div, dou, end, esej, fkp, gas, ge, hem, imm, inh, mep, min, mmo,
  nch, neo, nep, neu, nut, obg, obs, orl, ort, pal, ped, phy, pla, psc, pso, psy, pul, quickform,
  rams, rhu, rneu, rped, rphy, rsat, scol, sjm, soc, sto, tel, uro

Les tables jobs et jobs_sections sont traitées par le DAG `curated_cscmed_jobs`.

### Tests QA
Les tests QA anonymized sont répartis en 5 shards par test.

### Horaire
* __Date de début__ - 9 avril 2026
* __Jour et heure__ - Jeudi, 20h heure de Montréal
* __Intervalle__ - Chaque 4 semaines
"""

CURATED_DATASETS = [
    {"dataset_id": "curated_cscmed_quickform"                           , "cluster_type": "small", "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "curated_cscmed_gas_notemedicalebreve_v"             , "cluster_type": "small", "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "curated_cscmed_hem_bilan_inr_entete_p"              , "cluster_type": "small", "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "curated_cscmed_hem_thrombophilie_entete_p"          , "cluster_type": "small", "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "curated_cscmed_scol_autresparametresradiologiques_p", "cluster_type": "small", "run_type": "default", "pass_date": False, "dependencies": []}
]

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
    # temp
    {"dataset_id": "anonymized_cscmed_gas*"       , "cluster_type": "medium", "run_type": "initial", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_ge*"        , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_hem*"       , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
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
    {"dataset_id": "anonymized_cscmed_ped*"       , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
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
    {"dataset_id": "anonymized_cscmed_sjm*"       , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_soc*"       , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_sto*"       , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_tel*"       , "cluster_type": "small" , "run_type": "default", "pass_date": False, "dependencies": []},
    {"dataset_id": "anonymized_cscmed_uro*"       , "cluster_type": "small" , "run_type": "default", "pass_date": False, "dependencies": []}
]

_SIZE_RANK = {"large": 0, "medium": 1, "small": 2}
ANON_DESTINATIONS = [dataset["dataset_id"] for dataset in
                     sorted(ANON_DATASETS, key=lambda d: _SIZE_RANK[d["cluster_type"].strip()])]

# equal_counts is skipped for demographic: the anonymization intentionally filters out
# test-patient records (ExcludedTestIds), so its row count will always differ from raw by design.
EQUAL_COUNTS_DESTINATIONS = [d for d in ANON_DESTINATIONS if d != "anonymized_cscmed_demographic"]

QA_SHARDS = 5


def shard_tests(test_name: str, destinations: list, cluster_type: str) -> list:
    return [
        {"name": test_name, "destinations": destinations[shard::QA_SHARDS],
         "cluster_type": cluster_type, "suffix": str(shard + 1)}
        for shard in range(QA_SHARDS)
    ]


dag_config = {
    "steps": [
        {
            "destination_zone": "red",
            "destination_subzone": "curated",
            "main_class": "bio.ferlab.ui.etl.red.curated.Main",
            "multiple_main_methods": True,
            "pre_tests": [{"name": "greater_or_equal_partition_counts",
                           "destinations": [dataset["dataset_id"] for dataset in CURATED_DATASETS],
                           "cluster_type": "small"}],
            "datasets": CURATED_DATASETS,
            "optimize": [],
            "post_tests": []
        },
        {
            "destination_zone": "yellow",
            "destination_subzone": "anonymized",
            "main_class": "bio.ferlab.ui.etl.yellow.anonymized.Main",
            "multiple_main_methods": False,
            "pre_tests": shard_tests("greater_or_equal_partition_counts", ANON_DESTINATIONS, "medium"),
            "datasets": ANON_DATASETS,
            "optimize": [],
            "post_tests": shard_tests("lower_or_equal_null_counts", ANON_DESTINATIONS, "medium")
                          + shard_tests("equal_counts", EQUAL_COUNTS_DESTINATIONS, "medium")
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
    concurrency=5,
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
