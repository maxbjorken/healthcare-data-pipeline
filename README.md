# Pipeline for healthcare-data.

## Architecture:
### Dlt for transfering data
### Polars for dataingestion
### DuckDB for database
### Dbt for data transformation within the database
### Prefect for orchestration


## Getting Started

1. **Clone the repository:** `git clone https://github.com/maxbjorken/healthcare-data-pipeline.git`

2. **Create a virtual environment:** `python -m venv venv`

3. **Install dependencies:** `pip install -r requirements.txt`

4. **Download dbt packages:** `dbt deps`

5. **Run the pipeline:** `python HealthcarePipeline.py`