from icecream import ic
from dagster import op, io_manager, Out, get_dagster_logger
from my_io_managers import JsonLDFileIOManager

@io_manager(output_config_schema={"jsonld_file": str})
def second_task_io_manager(_):
    return JsonLDFileIOManager()

@op(out=Out(io_manager_key="second_task_io_manager"))
def second_task(context, first_task_result):
    """reverse sample data values"""
    logger = get_dagster_logger()
    logger.info("hello from second_task")
    ic(context.op_config)
    ic(first_task_result)

    second_task_result = []
    for _ in first_task_result:
        for k, v in _.items():
            rv = v
            rv.reverse()
            second_task_result.append({k: rv})
    ic(second_task_result)

    return second_task_result
