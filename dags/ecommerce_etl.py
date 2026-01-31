from airflow.decorators import dag, task
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from pathlib import Path
import pandas as pd

from src.extract import extract_data
from src.transform import transform_dataframes, validate_dataframes

DATABASE_URL = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="ecommerce_etl_v1",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["ecommerce", "etl"],
)
def ecommerce_dag():

    # --------------------------------------------------
    # SETUP DATABASE
    # --------------------------------------------------
    @task(do_xcom_push=False)
    def setup_db():
        """
        Executes SQL files to create schemas and tables.
        """
        engine = create_engine(DATABASE_URL)

        sql_files = [
            "/app/sql/schemas.sql",
            "/app/sql/staging_tables.sql",
            "/app/sql/core_tables.sql",
            "/app/sql/gold_tables.sql",
        ]

        with engine.begin() as conn:
            for file_path in sql_files:
                path = Path(file_path)
                if path.exists():
                    conn.execute(text(path.read_text()))
                else:
                    print(f"[WARN] SQL file not found: {file_path}")

    # --------------------------------------------------
    # EXTRACT
    # --------------------------------------------------
    @task(do_xcom_push=False)
    def extract():
        """
        Validates that raw data files exist and are readable.
        Does not return data.
        """
        extract_data()

    # --------------------------------------------------
    # TRANSFORM
    # --------------------------------------------------
    @task(do_xcom_push=False)
    def transform():
        """
        Transforms raw data and writes intermediate results to disk.
        """
        base_path = Path("/app/data/raw")

        df1 = pd.read_csv(base_path / "productsfull.csv")
        df2 = pd.read_csv(base_path / "productsfull2.csv")
        df3 = pd.read_csv(base_path / "productsclassified.csv")

        stg, core, gold = transform_dataframes(df1, df2, df3)
        validate_dataframes(stg, core, gold)

        # Final defensive cleanup against NaN values
        stg = stg.where(pd.notnull(stg), None)
        core = core.where(pd.notnull(core), None)
        gold = gold.where(pd.notnull(gold), None)

        stg.to_parquet("/tmp/stg.parquet", index=False)
        core.to_parquet("/tmp/core.parquet", index=False)
        gold.to_parquet("/tmp/gold.parquet", index=False)

    # --------------------------------------------------
    # LOAD
    # --------------------------------------------------
    @task(do_xcom_push=False)
    def load():
        """
        Loads transformed data into PostgreSQL schemas.
        """
        engine = create_engine(DATABASE_URL)

        pd.read_parquet("/tmp/stg.parquet").to_sql(
            name="products_1",
            con=engine,
            schema="staging",
            if_exists="replace",
            index=False,
        )

        pd.read_parquet("/tmp/core.parquet").to_sql(
            name="products_2",
            con=engine,
            schema="core",
            if_exists="replace",
            index=False,
        )

        pd.read_parquet("/tmp/gold.parquet").to_sql(
            name="products_classified",
            con=engine,
            schema="gold",
            if_exists="replace",
            index=False,
        )

    # DAG workflow
    setup_db() >> extract() >> transform() >> load()


ecommerce_etl_dag = ecommerce_dag()
