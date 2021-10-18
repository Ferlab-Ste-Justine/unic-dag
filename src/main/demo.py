from dagster import solid, pipeline, Output, OutputDefinition, ModeDefinition, fs_io_manager, multiprocess_executor, \
    default_executors
from dagster.experimental import DynamicOutput, DynamicOutputDefinition
import time
from random import randrange

@solid (config_schema={"time": int},
        output_defs=[
            OutputDefinition(name="common_data"),
            DynamicOutputDefinition(name="data")
        ])

def expensive_setup(context):
    context.log.info("Starting Expensive Setup ...")
    time.sleep(context.solid_config["time"])

    yield Output("This is a complicated data.",
                 output_name="common_data")
    for i in range(10):
        yield DynamicOutput(output_name="data", value=i, mapping_key=str(i))

@solid(config_schema={"time":int})
def expensive_analyzes(context, data_common, data):
    context.log.info("Doing something with a complicate data ...")
    time.sleep(context.solid_config["time"])

@pipeline(mode_defs=[
    ModeDefinition(resource_defs={"io_manager": fs_io_manager},
                   executor_defs=default_executors + [multiprocess_executor])
])
def expensive_pipeline():
    common_data, datas = expensive_setup()
    datas.map(lambda data: expensive_analyzes(common_data, data))