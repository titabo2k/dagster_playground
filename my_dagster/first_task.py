import json
from icecream import ic
from dagster import op, get_dagster_logger

@op
def first_task(context):
    """reading source jsonld file to
    provide list of sample objects"""
    logger = get_dagster_logger()
    logger.info("hello from first_task")
    ic(context.op_config)

    sample_data = []

    with open(
       context.op_config["jsonld_input_file"],
       mode="r",
       encoding="UTF-8") as fh_in:
       for _ in fh_in:
           sample_data.append(json.loads(_))
    ic(sample_data)

    return sample_data
