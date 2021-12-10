from dagster import job, resource, InputDefinition
from dagster_dbt import dbt_cli_resource, dbt_run_op, dbt_seed_op, dbt_test_op
from dagster_dbt.cli.types import DbtCliOutput
from dagster.utils import script_relative_path
import dagstermill as dm
from dagster import In, Nothing

my_dbt_resource = dbt_cli_resource.configured({"project_dir": "dbt-greenery"})

analysis = dm.define_dagstermill_op(
    "analysis",
    script_relative_path("analysis.ipynb"),
    output_notebook_name="analysis_output",
    input_defs=[InputDefinition("start", Nothing)]
)


@job(
    resource_defs={
        "dbt": my_dbt_resource, 
        "output_notebook_io_manager": dm.local_output_notebook_io_manager,
    }, 
    description = "My DBT Job"
)
def my_dbt_job():
    my_dbt_run_op = dbt_run_op(start_after=dbt_seed_op())
    my_dbt_test_op = dbt_test_op(start_after=my_dbt_run_op)
    analysis(start=my_dbt_test_op)