"""
Transfer DAG for the preterm NIfTIs going to SD4Health
"""

from datetime import datetime

from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule

from lib.config import DEFAULT_ARGS, LOCAL_TZ, NIFTI_PREFIX, VNA_CLINIQUE_YELLOW_BUCKET
from lib.nifti import study_selection_params
from lib.sd4h import CEPH_BUCKET
from lib.slack import Slack
from lib.tasks.nifti import resolve_studies
from lib.tasks.notify import start, end
from lib.tasks.sd4h import build_transfer_args, transfer_studies

DOC = f"""
# SD4H Preterm NIfTI Transfer DAG

DAG pour le transfert des IRMs anonymisées du projet preterm vers SD4Health.

### Description
Ce DAG copie les études NIfTI de `{VNA_CLINIQUE_YELLOW_BUCKET}/{NIFTI_PREFIX}/` vers le bucket
SD4Health `{CEPH_BUCKET}` sur Juno, à l'aide de `rclone`. La structure de date de la source est
conservée : une étude copiée garde le même chemin de part et d'autre.

Seules les études demandées sont transférées, jamais tout le bucket. Elles sont déterminées soit par
le paramètre `paths`, soit par un fichier CSV de numéros d'accès. Exactement un des deux est
obligatoire : fournir les deux est une erreur.

Le bucket et l'utilisateur MinIO du côté SD4Health portent encore le nom `brain-mri`, qui était celui
du projet preterm au moment de leur création.

### Horaire
* __Date de début__ - aucune
* __Date de fin__ - aucune
* __Jour et heure__ - aucun, déclenchement manuel seulement
* __Durée maximale__ - aucune, un lot de plusieurs milliers d'études peut durer des heures

### Reprise après interruption
Une run interrompue (échec, éviction du pod) n'a rien à reprendre manuellement : `rclone copy` ignore
les objets déjà identiques à destination, donc il suffit de redéclencher le DAG.

### Études pas encore converties
Les numéros d'accès demandés qui n'ont pas encore de NIfTI dans
`{VNA_CLINIQUE_YELLOW_BUCKET}/{NIFTI_PREFIX}/` sont ignorés plutôt que de faire échouer la run, et
sont listés dans un avertissement du log de `resolve_studies`. Redéclencher le DAG une fois la
conversion faite les transfère. La run échoue seulement si **aucune** étude demandée n'existe.

### Configuration
* Paramètre `paths` : Liste de préfixes d'études à transférer. Les jokers `*` sont permis dans
  n'importe quel segment. Ex: `{NIFTI_PREFIX}/2026/01/01/RA202600012345`,
  `{NIFTI_PREFIX}/2026/01/01/RA2026000*`, `{NIFTI_PREFIX}/2026/01/*`. Mutuellement exclusif avec
  `accession_file_key`.
* Paramètre `accession_file_bucket` : (Optionnel) Bucket contenant le fichier CSV de numéros d'accès.
  Doit être fourni avec `accession_file_key`.
* Paramètre `accession_file_key` : (Optionnel) Clé du fichier CSV de numéros d'accès. Doit être
  fourni avec `accession_file_bucket`. Mutuellement exclusif avec `paths`.
* Paramètre `accession_number_column` : Nom de la colonne des numéros d'accès dans le fichier CSV.
  Les jokers `*` sont permis dans les valeurs.
* Paramètre `exam_date_column` : Nom de la colonne des dates d'examen dans le fichier CSV. Les dates
  doivent être au format ISO 8601 (ex: `2026-01-15`). Tout autre format est rejeté plutôt que
  deviné : lire le jour comme le mois donnerait un préfixe qui ne correspond à aucune étude.
"""

# Update default args
args = DEFAULT_ARGS.copy()
args.update({
    'trigger_rule': TriggerRule.NONE_FAILED,
    'on_failure_callback': Slack.notify_task_failure})

with DAG(
        dag_id="sd4h_preterm_nifti_transfer",
        params=study_selection_params(parent_prefix=NIFTI_PREFIX, action="transfer"),
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
    resolve_studies_task = resolve_studies(bucket=VNA_CLINIQUE_YELLOW_BUCKET,
                                           parent_prefix=NIFTI_PREFIX,
                                           verify_objects=True)
    transfer_args = build_transfer_args(studies=resolve_studies_task)

    start("start_sd4h_preterm_nifti_transfer") >> resolve_studies_task >> transfer_args \
    >> transfer_studies(arguments=transfer_args) \
    >> end("end_sd4h_preterm_nifti_transfer")
