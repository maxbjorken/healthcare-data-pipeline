import subprocess
from prefect import task, get_run_logger

@task(name="dbt Build")
def run_dbt():
    logger = get_run_logger()
    
    result = subprocess.run(["dbt", "build"], capture_output=True, text=True)
    
    if result.stdout:
        logger.info(result.stdout)
    
    if result.returncode == 0:
        logger.info("✅ Dbt build success!")
    else:
        logger.error("❌ Dbt build failed!")
        logger.error(result.stderr)
        raise Exception("Dbt build failed")