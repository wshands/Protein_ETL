"""
## Protein ETL example DAG

This example demonstrates an ETL pipeline using Airflow.
The pipeline extracts protein data from Uniprot, transforms it by filtering based on
protein name, and loads the filtered data into a Postgres database.
"""
  # This DAG uses the TaskFlow API. See: https://www.astronomer.io/docs/learn/airflow-decorators
from airflow.sdk import Asset, chain, Param, dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime as builtins_datetime
from pendulum import datetime, duration
from requests.adapters import HTTPAdapter, Retry

import logging
import os
import psycopg
import re
import requests


# use the Airflow task logger to log information to the task logs (or use print())
t_log = logging.getLogger("airflow.task")

# define variables used in a DAG as environment variables in .env for your whole Airflow instance
# to standardize your DAGs
_PROTEIN_PARAMETER_NAME = "Protein name"
_PROTEIN_NAME_DEFAULT = "cdk3"
_POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID", "postgres_default")

# -------------- #
# DAG Definition #
# -------------- #
# instantiate a DAG with the @dag decorator and set DAG parameters (see: https://www.astronomer.io/docs/learn/airflow-dag-parameters)
@dag(
    start_date=datetime(2025, 4, 1),  # date after which the DAG can be scheduled
    schedule="@daily",  # see: https://www.astronomer.io/docs/learn/scheduling-in-airflow for options
    max_consecutive_failed_dag_runs=5,  # auto-pauses the DAG after 5 consecutive failed runs, experimental
    max_active_runs=1,  # only allow one concurrent run of this DAG, prevents parallel DuckDB calls
    doc_md=__doc__,  # add DAG Docs in the UI, see https://www.astronomer.io/docs/learn/custom-airflow-ui-docs-tutorial
    default_args={
        "owner": "Astro",  # owner of this DAG in the Airflow UI
        "retries": 3,  # tasks retry 3 times before they fail
        "retry_delay": duration(seconds=30),  # tasks wait 30s in between retries
    },  # default_args are applied to all tasks in a DAG
    tags=["example", "ETL"],  # add tags in the UI
    params={  # Airflow params can add interactive options on manual runs. See: https://www.astronomer.io/docs/learn/airflow-params
        _PROTEIN_PARAMETER_NAME: Param(
            _PROTEIN_NAME_DEFAULT,
            type="string",
            title="Protein Name",
            description="Set the protein name to filter the data.",
        )
    },
    # Warning - in-memory DuckDB is not a persistent database between workers. To move this workflow in production, use a
    # cloud-based database and based on concurrency capabilities adjust the two parameters below.
    is_paused_upon_creation=False, # start running the DAG as soon as its created
)


def protein_etl():  # by default the dag_id is the name of the decorated function

    # ---------------- #
    # Task Definitions #
    # ---------------- #
    # the @task decorator turns any Python function into an Airflow task
    # any @task decorated function that is called inside the @dag decorated
    # function is automatically added to the DAG.
    # if one exists for your use case you can still use traditional Airflow operators
    # and mix them with @task decorators. Checkout registry.astronomer.io for available operators
    # see: https://www.astronomer.io/docs/learn/airflow-decorators for information about @task
    # see: https://www.astronomer.io/docs/learn/what-is-an-operator for information about traditional operators

    @task(retries=2)  # you can override default_args at the task level
    def extract_protein_data(**context) -> list:
        """
        Retrieve data about galaxies.
        This task simulates an extraction step in an ETL pipeline.
        Args:
            num_galaxies (int): The number of galaxies for which data should be returned.
            Default is 20. Maximum is 20.
        Returns:
            pd.DataFrame: A DataFrame containing data about galaxies.
        """
        # retrieve param values from the context
        protein_name = context["params"][
            _PROTEIN_PARAMETER_NAME
        ]

        # API URL using the search endpoint
        # This endpoint is lighter and returns chunks of 500 at a time and requires pagination
        url = f'https://rest.uniprot.org/uniprotkb/search?compressed=false&format=fasta&query=%28{protein_name}%29&size=500'

        re_next_link = re.compile(r'<(.+)>; rel="next"')
        retries = Retry(total=5, backoff_factor=0.25, status_forcelist=[500, 502, 503, 504])
        session = requests.Session()
        session.mount("https://", HTTPAdapter(max_retries=retries))

        def get_next_link(headers):
            if "Link" in headers:
                match = re_next_link.match(headers["Link"])
                if match:
                    return match.group(1)

        def get_batch(batch_url):
            while batch_url:
                response = session.get(batch_url)
                response.raise_for_status()
                total = response.headers["x-total-results"]
                yield response, total
                batch_url = get_next_link(response.headers)

        for batch, total in get_batch(url):
            # Process the batch of data
            all_fastas = batch.text
            # Split the FASTA data into individual entries
            fasta_list = re.split(r'\n(?=>)', all_fastas)
            t_log.info(f"Total FASTA entries: {total}")
            t_log.info(f"Number of FASTA entries in this batch: {len(fasta_list)}")

        return fasta_list

    @task
    def transform_protein_data(fasta_list: list) -> list[dict]:
        """
        Filter the galaxy data based on the distance from the Milky Way.
        This task simulates a transformation step in an ETL pipeline.
        Args:
            closeness_threshold_light_years (int): The threshold for filtering
            galaxies based on distance.
            Default is 500,000 light years.
        Returns:
            pd.DataFrame: A DataFrame containing filtered galaxy data.
        """
        filtered_protein_list = [fasta for fasta in fasta_list if 'Homo sapiens' in fasta]

        t_log.info("Filtering protiens for 'Homo sapiens'.")

        filtered_protein_list_of_dict = [
               {'name': s.split('\n', 1)[0], 'sequence': s.split('\n', 1)[1]} if '\n' in s else {'name': s, 'sequence': ''} for s in filtered_protein_list]
   
        return filtered_protein_list_of_dict


    @task
    def load_protein_data(filtered_protein_list: list[dict]):
        t_log.info(filtered_protein_list)

        pg_hook = PostgresHook(postgres_conn_id=_POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        for filtered_protein in filtered_protein_list:
            cursor.execute("""
                       INSERT INTO "ATLAS_protein" (name, sequence, created_at) VALUES (%s, %s, %s)
                       """, (
                            filtered_protein["name"],
                            filtered_protein["sequence"],
                            builtins_datetime.now()
                       ))
        conn.commit()
        cursor.close()


    # ------------------------------------ #
    # Calling tasks + Setting dependencies #
    # ------------------------------------ #

    # each call of a @task decorated function creates one task in the Airflow UI
    # passing the return value of one @task decorated function to another one
    # automatically creates a task dependency
    extract_protein_data_obj = extract_protein_data()
    transform_protein_data_obj = transform_protein_data(extract_protein_data_obj)
    load_galaxy_data_obj = load_protein_data(transform_protein_data_obj)


# Instantiate the DAG
protein_etl()
