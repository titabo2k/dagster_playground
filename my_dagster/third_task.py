from icecream import ic
from dagster import op, io_manager, Out, get_dagster_logger
from my_io_managers import JsonLDFileIOManager

@io_manager(output_config_schema={"jsonld_file": str})
def third_task_io_manager(_):
    return JsonLDFileIOManager()

@op(out=Out(io_manager_key="third_task_io_manager"))
def third_task(context, second_task_result):
    """upper case sample data keys"""
    logger = get_dagster_logger()
    logger.info("hello from third_task")
    ic(context.op_config)
    ic(second_task_result)

    third_task_result = []
    for _ in second_task_result:
        for k, v in _.items():
            third_task_result.append({k.upper(): v})
    ic(third_task_result)

    return third_task_result
