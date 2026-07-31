"""
NIfTI conversion DAG for the VNA clinique brain imaging DICOMs
"""

from datetime import datetime

from airflow import DAG
from airflow.models import Param
from airflow.utils.trigger_rule import TriggerRule

from lib.config import DEFAULT_ARGS, LOCAL_TZ
from lib.slack import Slack
from lib.tasks.nifti import convert_studies, get_skip_existing, resolve_studies
from lib.tasks.notify import start, end

DOC = """
# Anonymized VNA Clinique NIfTI DAG

DAG pour la conversion en NIfTI des images cérébrales DICOM du VNA clinique.

### Description
Ce DAG convertit les études DICOM de `vna-clinique-red/dicoms/` en NIfTI compressés à l'aide de
`dcm2niix`. La structure de date de la source est conservée dans les sorties :

* `vna-clinique-yellow/nifti/<année>/<mois>/<jour>/<numéro d'accès>/` : les NIfTI (`.nii.gz`) et les
  sidecars JSON **anonymisés**.
* `vna-clinique-red/nifti_sidecars/<année>/<mois>/<jour>/<numéro d'accès>/` : les sidecars JSON
  **nominatifs**, qui contiennent les identifiants du patient et les dates d'acquisition.

Les études à convertir sont déterminées soit par le paramètre `paths`, soit par un fichier CSV de
numéros d'accès. Exactement un des deux est obligatoire : fournir les deux est une erreur.

### Horaire
* __Date de début__ - aucune
* __Date de fin__ - aucune
* __Jour et heure__ - aucun, déclenchement manuel seulement
* __Durée maximale__ - aucune, un backfill peut durer plusieurs jours

### Reprise après interruption
Une run interrompue (échec, éviction du pod) n'a rien à reprendre manuellement : avec `skip_existing` à
`True`, il suffit de redéclencher le DAG et les études déjà converties sont ignorées. Un run qui
rapporte 0 étude convertie signifie que le lot est terminé.

### Configuration
* Paramètre `paths` : Liste de préfixes d'études à convertir. Les jokers `*` sont permis dans
  n'importe quel segment. Ex: `dicoms/2026/01/01/RA202600012345`, `dicoms/2026/01/01/RA2026000*`,
  `dicoms/2026/01/*`. Mutuellement exclusif avec `accession_file_key`.
* Paramètre `accession_file_bucket` : (Optionnel) Bucket contenant le fichier CSV de numéros d'accès.
  Doit être fourni avec `accession_file_key`.
* Paramètre `accession_file_key` : (Optionnel) Clé du fichier CSV de numéros d'accès. Doit être
  fourni avec `accession_file_bucket`. Mutuellement exclusif avec `paths`.
* Paramètre `accession_number_column` : Nom de la colonne des numéros d'accès dans le fichier CSV.
  Les jokers `*` sont permis dans les valeurs.
* Paramètre `exam_date_column` : Nom de la colonne des dates d'examen dans le fichier CSV. Les dates
  doivent être au format ISO 8601 (ex: `2026-01-15`). Tout autre format est rejeté plutôt que
  deviné : lire le jour comme le mois donnerait un préfixe qui ne correspond à aucune étude.
* Paramètre `skip_existing` : Si `True`, les études dont les NIfTI existent déjà sont laissées
  intactes. Si `False`, ces sorties sont **supprimées** puis reconverties.

### Sidecars des études déjà converties
Les études converties avant ce DAG n'ont pas de sidecars JSON. Avec `skip_existing` à `True` elles
seront toujours ignorées; les reconvertir avec `skip_existing` à `False` est le seul moyen de leur
générer des sidecars.
"""

# Update default args
args = DEFAULT_ARGS.copy()
args.update({
    'trigger_rule': TriggerRule.NONE_FAILED,
    'on_failure_callback': Slack.notify_task_failure,
    'retries': 0})  # With skip_existing to False, a retry would delete and reconvert every study again

with DAG(
        dag_id="anonymized_vna_clinique_nifti",
        params={
            "paths": Param([], type=["null", "array"],
                           description="Study prefixes to convert, one per line. Wildcards allowed in any segment. "
                                       "Ex: dicoms/2026/01/01/RA202600012345, dicoms/2026/01/01/RA2026000*, "
                                       "dicoms/2026/01/*. Mutually exclusive with 'accession_file_key'."),
            "accession_file_bucket": Param(None, type=["null", "string"],
                                           description="(Optional) Bucket holding the accession number CSV file. Required with 'accession_file_key'."),
            "accession_file_key": Param(None, type=["null", "string"],
                                        description="(Optional) Key of the accession number CSV file. Required with 'accession_file_bucket'. Mutually exclusive with 'paths'."),
            "accession_number_column": Param("accessionNumber", type="string",
                                             description="Accession number column in the CSV file. Wildcards allowed in values."),
            "exam_date_column": Param("examDate", type="string",
                                      description="Exam date column in the CSV file. Must be ISO 8601. Ex: 2026-01-15"),
            "skip_existing": Param(True, type="boolean",
                                   description="Skip studies whose NIfTI output already exists. If False, that output is deleted and converted again."),
        },
        default_args=args,
        doc_md=DOC,
        start_date=datetime(2026, 7, 29, tzinfo=LOCAL_TZ),
        is_paused_upon_creation=True,
        schedule_interval=None,
        catchup=False,
        max_active_runs=1,
        tags=["anonymized"],
        on_failure_callback=Slack.notify_dag_failure
) as dag:
    resolve_studies_task = resolve_studies()
    get_skip_existing_task = get_skip_existing()

    start("start_anonymized_vna_clinique_nifti") >> [resolve_studies_task, get_skip_existing_task] \
    >> convert_studies(studies=resolve_studies_task, skip_existing=get_skip_existing_task) \
    >> end("end_anonymized_vna_clinique_nifti")
