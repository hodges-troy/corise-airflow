from datetime import datetime
from typing import List

import pandas as pd
from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator

from common.week_3.config import DATA_TYPES, normalized_columns


PROJECT_ID = "corise-airflow-2023"
DESTINATION_BUCKET = "corise-airflow-tjh"
BQ_DATASET_NAME = "energy_prediction"
LOCATION = "us-west2"

@dag(
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
)
def data_warehouse_transform_dag():
    """
    ### Data Warehouse Transform DAG
    This DAG performs four operations:
        1. Extracts zip file into two dataframes
        2. Loads these dataframes into parquet files on GCS, with valid column names
        3. Builds external tables on top of these parquet files
        4. Builds normalized views on top of the external tables
        5. Builds a joined view on top of the normalized views, joined on time
    """

    @task
    def extract() -> List[pd.DataFrame]:
        """
        #### Extract task
        A simple task that loads each file in the zipped file into a dataframe,
        building a list of dataframes that is returned
        """
        from zipfile import ZipFile
        filename = "/usr/local/airflow/dags/data/energy-consumption-generation-prices-and-weather.zip"
        dfs = [pd.read_csv(ZipFile(filename).open(i)) for i in
               ZipFile(filename).namelist()]
        return dfs

    @task
    def load(unzip_result: List[pd.DataFrame]):
        """
        #### Load task
        A simple "load" task that takes in the result of the "extract" task, formats
        columns to be BigQuery-compliant, and writes data to GCS.
        """

        from airflow.providers.google.cloud.hooks.gcs import GCSHook

        client = GCSHook().get_conn()
        bucket = client.get_bucket(DESTINATION_BUCKET)

        for index, df in enumerate(unzip_result):
            df.columns = df.columns.str.replace(" ", "_")
            df.columns = df.columns.str.replace("/", "_")
            df.columns = df.columns.str.replace("-", "_")
            bucket.blob(
                f"week-3/{DATA_TYPES[index]}.parquet").upload_from_string(
                df.to_parquet(), "text/parquet")
            print(df.dtypes)

    @task_group
    def create_bigquery_dataset():
        from airflow.providers.google.cloud.operators.bigquery import \
            BigQueryCreateEmptyDatasetOperator
        # EmptyOperator(task_id='placeholder')
        # TODO Modify here to create a BigQueryDataset if one does not already exist
        # This is where your tables and views will be created
        BigQueryCreateEmptyDatasetOperator(
            task_id='create-bq-dataset',
            dataset_id=BQ_DATASET_NAME,
            project_id=PROJECT_ID,
            exists_ok=True,
            location=LOCATION,
        )

    @task_group
    def create_external_tables():
        from airflow.providers.google.cloud.operators.bigquery import \
            BigQueryCreateExternalTableOperator
        # EmptyOperator(task_id='placeholder')

        # TODO Modify here to produce two external tables, one for each data type, referencing the data stored in GCS

        # When using the BigQueryCreateExternalTableOperator, it's suggested you use the table_resource
        # field to specify DDL configuration parameters. If you don't, then you will see an error
        # related to the built table_resource specifying csvOptions even though the desired format is
        # PARQUET.
        tasks = []
        for data_type in DATA_TYPES:

            table_resource = {
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BQ_DATASET_NAME,
                    "tableId": f"{data_type}_external",
                },
                "externalDataConfiguration": {
                    "sourceUris":
                        [
                            f"gs://{DESTINATION_BUCKET}/week-3"
                            f"/{data_type}.parquet"
                        ],
                    "sourceFormat": "PARQUET",
                },
            }
            tasks.append(
                BigQueryCreateExternalTableOperator(
                    task_id=f"create-{data_type}-external-table",
                    table_resource=table_resource,
                    location=LOCATION,
                )
            )

    def produce_select_statement(timestamp_column: str,
                                 columns: List[str]) -> str:
        # TODO Modify here to produce a select statement by casting 'timestamp_column' to
        # TIMESTAMP type, and selecting all of the columns in 'columns'
        return f"""
        SELECT 
            CAST({timestamp_column} AS TIMESTAMP) AS {timestamp_column},
            {', '.join(columns)}
        """

    @task_group
    def produce_normalized_views():
        from airflow.providers.google.cloud.operators.bigquery import \
            BigQueryCreateEmptyTableOperator
        # TODO Modify here to produce views for each of the datasources, capturing only the essential
        # columns specified in normalized_columns. A key step at this stage is to convert the relevant
        # columns in each datasource from string to time. The utility function 'produce_select_statement'
        # accepts the timestamp column, and essential columns for each of the datatypes and build a
        # select statement ptogrammatically, which can then be passed to the Airflow Operators.
        # EmptyOperator(task_id='placeholder')
        tasks = []
        for data_type in DATA_TYPES:
            partial_select_statement = produce_select_statement(
                timestamp_column=normalized_columns[data_type]["time"],
                columns=normalized_columns[data_type]["columns"]
            )
            full_select_statement = (
                partial_select_statement +
                f" FROM {PROJECT_ID}.{BQ_DATASET_NAME}.{data_type}_external;"
            )
            tasks.append(
                BigQueryCreateEmptyTableOperator(
                    task_id=f"create-{data_type}-normalized-view",
                    dataset_id=BQ_DATASET_NAME,
                    table_id=f"{data_type}_normalized",
                    view={
                        "query": full_select_statement,
                        "useLegacySql": False,
                    },
                    location=LOCATION
                )
            )

    @task_group
    def produce_joined_view():
        from airflow.providers.google.cloud.operators.bigquery import \
            BigQueryCreateEmptyTableOperator
        # TODO Modify here to produce a view that joins the two normalized views on time
        # EmptyOperator(task_id='placeholder')

        # join two normalized views on time
        select_statement = f"""
        SELECT
            a.*,
            b.* except ({normalized_columns[DATA_TYPES[1]]["time"]})
            FROM {PROJECT_ID}.{BQ_DATASET_NAME}.{DATA_TYPES[0]}_normalized AS a
            JOIN {PROJECT_ID}.{BQ_DATASET_NAME}.{DATA_TYPES[1]}_normalized AS b
            ON a.{normalized_columns[DATA_TYPES[0]]["time"]} = b.{normalized_columns[DATA_TYPES[1]]["time"]}
        """
        # create joined view
        BigQueryCreateEmptyTableOperator(
            task_id=f"create-joined-view",
            dataset_id=BQ_DATASET_NAME,
            table_id=f"{DATA_TYPES[0]}_{DATA_TYPES[1]}_normalized",
            view={
                "query": select_statement,
                "useLegacySql": False,
            },
            location=LOCATION
        )

    unzip_task = extract()
    load_task = load(unzip_task)
    create_bigquery_dataset_task = create_bigquery_dataset()
    load_task >> create_bigquery_dataset_task
    external_table_task = create_external_tables()
    create_bigquery_dataset_task >> external_table_task
    normal_view_task = produce_normalized_views()
    external_table_task >> normal_view_task
    joined_view_task = produce_joined_view()
    normal_view_task >> joined_view_task


data_warehouse_transform_dag = data_warehouse_transform_dag()