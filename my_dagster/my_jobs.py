from icecream import ic
from dagster import job

from first_task import first_task
from second_task import second_task, second_task_io_manager
from third_task import third_task, third_task_io_manager

@job(resource_defs={
    "third_task_io_manager": third_task_io_manager,
    "second_task_io_manager": second_task_io_manager})
def first_job():
    third_task(second_task(first_task()))

if __name__ == "__main__":
    config = {
        "ops": {
            "first_task": {
                "config": {
                    "jsonld_input_file": "./first_task_input.ldj"
                }
            },
            "second_task": {
                "config": {
                    "parrot": "norwegian blue",
                    "two weapons of the spanish inquisition": [
                        "fear",
                        "suprise",
                        "ruthless efficiency"]
                },
                "outputs": {"result": {
                    "jsonld_file": "./second_task_result.ldj"}}
            },
            "third_task": {
                "config": {
                    "spam": "ham",
                    "say no more": "nudge nudge"
                },
                "outputs": {"result": {
                    "jsonld_file": "./third_task_result.ldj"}}
            }
        }
    }

    ic(config)
    first_job.execute_in_process(run_config=config)