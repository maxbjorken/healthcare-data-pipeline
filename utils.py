# utils.py exempel
import subprocess
from prefect import task

@task(name="dbt Build")
def run_dbt():
    # Vi använder 'dbt build' eftersom det kör både RUN och TEST
    result = subprocess.run(["dbt", "build"], capture_output=True, text=True)
    if result.returncode == 0:
        print("dbt success!")
        print(result.stdout)
    else:
        print("dbt failed!")
        print(result.stderr)
        raise Exception("dbt build failed")