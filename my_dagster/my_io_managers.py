import json
from icecream import ic
from dagster import IOManager, get_dagster_logger

class JsonLDFileIOManager(IOManager):
    """read from and write to jsonld files"""
    def __init__(self):
        super().__init__()

        self.logger = get_dagster_logger()

    def handle_output(self, context, obj):
        self.logger.info("hello from JsonLDFileIOManager.handle_output")
        jsonld_file = context.config["jsonld_file"]
        ic(jsonld_file)
        ic(obj)

        with open(
            jsonld_file,
            mode="w",
            encoding="UTF-8") as fh_out:
            for _ in obj:
                fh_out.write(f"{json.dumps(_)}\n")

    def load_input(self, context):
        self.logger.info("hello from JsonLDFileIOManager.load_input")
        ic(context.upstream_output.config)
        jsonld_file = context.upstream_output.config["jsonld_file"]

        jsonld_data = []

        with open(
            jsonld_file,
            mode="r",
            encoding="UTF-8") as fh_in:
            for line in fh_in:
                jsonld_data.append(json.loads(line))

        ic(jsonld_data)

        return jsonld_data
