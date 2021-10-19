import json
from dagster import solid, pipeline, Output, OutputDefinition, ModeDefinition, fs_io_manager, multiprocess_executor, \
    default_executors
from dagster.experimental import DynamicOutput, DynamicOutputDefinition
import time
from random import randrange


def read_json_file(file_path):
    with open(file_path) as f:
        return json.load(f)

@solid (output_defs=[
    OutputDefinition(name="common_data"),
    DynamicOutputDefinition(name="data")
])

def expensive_setup(context):
    context.log.info("Starting Expensive Setup ...")
    data = read_json_file("src/main/jobs.json")
    time.sleep(randrange(5))

    yield Output("This is a complicated data.",
                 output_name="common_data")
    for job in data['jobs']:
        yield DynamicOutput(output_name="data", value=job["source"], mapping_key=job["source"])

@solid
def expensive_analyzes(context, data_common, data):
    context.log.info("Doing something with a complicate data ...")
    time.sleep(randrange(5))

@pipeline(mode_defs=[
    ModeDefinition(resource_defs={"io_manager": fs_io_manager},
                   executor_defs=default_executors + [multiprocess_executor])
])
def expensive_pipeline():
    common_data, datas = expensive_setup()
    datas.map(lambda data: expensive_analyzes(common_data, data))