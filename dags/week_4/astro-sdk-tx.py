import pandas as pd
from airflow.models.dag import DAG
from airflow.utils import timezone

import astro.sql as aql
from astro.files import File
from astro.table import Metadata, Table

from common.week_3.config import DATA_TYPES


BQ_DATASET_NAME = "energy_prediction"
GCP_CONNECTION_ID = "google_cloud_default"


time_columns = {
    "generation": "time",
    "weather": "dt_iso"
}

filepaths = {
    "generation": "gs://corise-airflow-tjh/week-3/generation.parquet",
    "weather": "gs://corise-airflow-tjh/week-3/weather.parquet"
}


@aql.dataframe
def extract_nonzero_columns(input_df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter out columns that have only 0 or null values by 
    calling fillna(0) and only selecting columns that have any non-zero elements
    Fill null values with 0 for filtering and extract only columns that have
    """
    # TODO Modify here
    input_df_filled = input_df.fillna(0)
    # select columns that have any non-zero elements from original DF without
    # filled null values. leaving zero values in can have unintended
    # consequences on our features, like temperature. will leave NAs in until
    # conversation with downstream teams on how to handle them.
    nonzero_columns = input_df.loc[:, (input_df_filled != 0).any(axis=0)]
    return nonzero_columns


@aql.transform
def convert_timestamp_columns(input_table: Table, data_type: str) -> Table:
    """
    Return a SQL statement that selects the input table elements, 
    casting the time column specified in 'time_columns' to TIMESTAMP
    """
    # TODO Modify here
    # parquet file has time column as string. Cast to TIMESTAMP.
    return f"""
    SELECT
        TIMESTAMP({time_columns[data_type]}) AS {time_columns[data_type]},
        * except ({time_columns[data_type]})
    FROM {{{{input_table}}}}
    """


@aql.transform
def join_tables(generation_table: Table, weather_table: Table) -> Table:  # skipcq: PYL-W0613
    """
    Join `generation_table` and `weather_table` tables on time to create an output table
    """
    # TODO Modify here
    return f"""
    SELECT
        g.*,
        w.* except (dt_iso)
    FROM {{{{generation_table}}}} g
    JOIN {{{{weather_table}}}} w
    ON g.time = w.dt_iso
    """

              
with DAG(
    dag_id="astro_sdk_transform_dag",
    schedule_interval=None,
    start_date=timezone.datetime(2022, 1, 1),
) as dag:
    """
    ### Astro SDK Transform DAG
    This DAG performs four operations:
        1. Loads parquet files from GCS into BigQuery, referenced by a Table object using aql.load_file
        2. Extracts nonzero columns from that table, using a custom Python function extending aql.dataframe
        3. Converts the timestamp column from that table, using a custom SQL statement extending aql.transform
        4. Joins the two tables produced at step 3 for each datatype on time

    Note that unlike other projects, the relations between objects is left out for you so you can get a more intuitive
    sense for how to work with Astro SDK. For some examples of how it can be used, check out 
    # https://github.com/astronomer/astro-sdk/blob/main/python-sdk/example_dags/example_google_bigquery_gcs_load_and_save.py
    """
    # TODO Modify here
    convert_timestamp_tasks = []
    for data_type in DATA_TYPES:
        # Load parquet files from GCS into BigQuery using Table object
        load_task = aql.load_file(
            task_id=f"load_{data_type}",
            input_file=File(path=filepaths[data_type]),
            output_table=Table(
                name=f"{data_type}_raw",
                metadata=Metadata(schema=BQ_DATASET_NAME),
                conn_id=GCP_CONNECTION_ID,
            )
        )
        # Extract nonzero columns from that table, store as temporary table
        # in BQ
        extract_col_task = extract_nonzero_columns(
            # task_id=f"extract_nonzero_{data_type}",
            input_df=load_task,
            output_table=Table(
                metadata=Metadata(schema=BQ_DATASET_NAME),
                conn_id=GCP_CONNECTION_ID,
            )
        )
        # Convert the timestamp column from that table, store as temporary
        # table in BQ
        convert_timestamp_tasks.append(
            convert_timestamp_columns(
                input_table=extract_col_task,
                data_type=data_type,
                output_table=Table(
                    metadata=Metadata(schema=BQ_DATASET_NAME),
                    conn_id=GCP_CONNECTION_ID,
                )
            )
        )
    # Join the two tables produced at step 3 for each datatype on time
    join_gen_weather_tables = join_tables(
        generation_table=convert_timestamp_tasks[0],
        weather_table=convert_timestamp_tasks[1],
        output_table=Table(
            name=f"{DATA_TYPES[0]}_{DATA_TYPES[1]}_native",
            metadata=Metadata(schema=BQ_DATASET_NAME),
            conn_id=GCP_CONNECTION_ID,
        )
    )
    # Cleans up all temporary tables produced by the SDK
    cleanup_temp_tables = aql.cleanup()
    join_gen_weather_tables >> cleanup_temp_tables

