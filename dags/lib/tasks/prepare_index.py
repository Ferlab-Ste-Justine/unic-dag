from lib.operators.spark import SparkOperator

PREPARE_INDEX_MAIN_CLASS = 'bio.ferlab.clin.etl.es.PrepareIndex'


def resource_centric(spark_jar: str, skip: str = '', task_id: str = 'gene_centric', **kwargs) -> SparkOperator:
    return SparkOperator(
        entrypoint='resource_centric',
        task_id=task_id,
        name='etl-prepare-resource-centric',
        steps='initial',
        app_name='etl_prepare_gene_centric',
        spark_class=PREPARE_INDEX_MAIN_CLASS,
        spark_config='config-etl-large',
        spark_jar=spark_jar,
        skip=skip,
        **kwargs
    )