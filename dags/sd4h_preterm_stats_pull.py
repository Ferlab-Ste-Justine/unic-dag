"""
Pull DAG for the preterm FreeSurfer stats produced on SD4Health
"""

from datetime import datetime

from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule

from lib.config import DEFAULT_ARGS, FREESURFER_STATS_PREFIX, LOCAL_TZ, VNA_CLINIQUE_YELLOW_BUCKET
from lib.sd4h import VM_HOST
from lib.slack import Slack
from lib.tasks.notify import start, end
from lib.tasks.sd4h import pull_stats

DOC = f"""
# SD4H Preterm Stats Pull DAG

DAG pour le rapatriement des fichiers `.stats` FreeSurfer du projet preterm depuis SD4Health.

### Description
Ce DAG copie les fichiers `.stats` produits par FreeSurfer sur la VM SD4Health (`{VM_HOST}`, en SFTP)
vers `{VNA_CLINIQUE_YELLOW_BUCKET}/{FREESURFER_STATS_PREFIX}/`, à l'aide de `rclone`. Seuls les
fichiers `.stats` sont rapatriés; le reste de l'arborescence FreeSurfer reste sur la VM.

Le chemin exact de l'arborescence FreeSurfer sur la VM n'est pas encore confirmé par l'équipe
SD4Health : `FREESURFER_STATS_PATH` dans `lib/sd4h.py` est encore un placeholder, et le DAG reste en
pause jusqu'à ce qu'il soit remplacé.

L'utilisateur MinIO utilisé porte encore le nom `brain-mri`, qui était celui du projet preterm au
moment de sa création.

### Horaire
* __Date de début__ - aucune
* __Date de fin__ - aucune
* __Jour et heure__ - aucun, déclenchement manuel seulement
* __Durée maximale__ - aucune

### Reprise après interruption
Une run interrompue n'a rien à reprendre manuellement : `rclone copy` ignore les fichiers déjà
identiques à destination, donc il suffit de redéclencher le DAG.
"""

# Update default args
args = DEFAULT_ARGS.copy()
args.update({
    'trigger_rule': TriggerRule.NONE_FAILED,
    'on_failure_callback': Slack.notify_task_failure})

with DAG(
        dag_id="sd4h_preterm_stats_pull",
        default_args=args,
        doc_md=DOC,
        start_date=datetime(2026, 8, 13, tzinfo=LOCAL_TZ),
        is_paused_upon_creation=True,
        schedule_interval=None,
        catchup=False,
        max_active_runs=1,
        tags=["anonymized", "sd4h"],
        on_failure_callback=Slack.notify_dag_failure
) as dag:
    start("start_sd4h_preterm_stats_pull") >> pull_stats() \
    >> end("end_sd4h_preterm_stats_pull")
