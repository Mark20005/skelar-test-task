"""
ETL pipeline for fetching and storing currency exchange rates.

This DAG implements an idempotent pipeline to download daily
currency exchange rates from the frankfurter.app API
and store them as partitioned Parquet files in GCS.
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict

import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from helpers.constants import API_URL
from requests.exceptions import RequestException

logger = logging.getLogger(__name__)

# --- DAG Configuration ---
# assuming that DAG should be parametrised to avoid hardcoding currencies, etc.
try:
    BASE_CURRENCY: str = Variable.get("base_currency")
    OUTPUT_PATH: str = Variable.get("output_path")
except KeyError as e:
    logger.error(f"Could not load critical Airflow Variable! Missing key: {e}")
    raise AirflowException(
        f"Failed to load Airflow Variable: {e}. Please set it in Airflow UI."
    )


@task(retries=5, retry_delay=timedelta(minutes=1))
def extract(logical_date_str: str, base_currency: str) -> Dict[str, Any]:
    """
    Extracts currency exchange rates for a specific logical date.

    Args:
        logical_date_str: The date for which to fetch rates,
                          in 'YYYY-MM-DD' format (from Airflow's {{ ds }}).
        base_currency: The base currency (e.g., 'USD') to fetch rates against.

    Returns:
        A dictionary containing the API response (JSON).

    Raises:
        AirflowException: If the API request fails (e.g., 4xx/5xx error).
    """
    endpoint = f"{API_URL}/{logical_date_str}"
    params = {"base": base_currency}

    logger.info(f"Requesting data from {endpoint} for base {base_currency}...")

    try:
        response = requests.get(endpoint, params=params)
        # this will raise an HTTPError for 4xx/5xx responses
        response.raise_for_status()
        data = response.json()

        logger.info(f"Successfully received data for date: {data.get('date')}")
        return data

    except RequestException as e:
        logger.error(f"Error during API request: {e}")
        raise AirflowException(f"API request failed: {e}")


@task
def transform(
    data: Dict[str, Any], logical_date_str: str, base_currency: str
) -> pd.DataFrame:
    """
    Transforms the raw API response (JSON) into a Pandas DataFrame
    with the correct schema and column order.

    Args:
        data: The JSON dictionary returned from the 'extract' task.
        logical_date_str: The logical date string, used as a fallback
                          if the API response is missing a date.
        base_currency: The base currency used for this request.

    Returns:
        A Pandas DataFrame with columns in the correct analytical order:
        ['date', 'base_currency', 'currency_code', 'rate'].

    Raises:
        ValueError: If the API response contains no rate data.
    """
    rates = data.get("rates", {})
    if not rates:
        logger.warning(f"API returned no rates for {logical_date_str}.")
        # raise ValueError to fail the task if no data is present
        raise ValueError(f"No rate data available for {logical_date_str}")

    df = pd.DataFrame(list(rates.items()), columns=["currency_code", "rate"])

    df["base_currency"] = base_currency
    df["date"] = pd.to_datetime(data.get("date", logical_date_str))

    # logical order of columns
    final_columns = ["date", "base_currency", "currency_code", "rate"]
    df = df[final_columns]

    logger.info(f"Transformed {len(df)} rows. Final columns: {final_columns}")
    return df


@task
def load(df: pd.DataFrame, logical_date_str: str, base_currency: str):
    """
    Saves the DataFrame as a Parquet file to GCS with Hive-style partitioning.

    This task explicitly fetches GCP credentials from the 'gcp_default'
    Airflow connection to ensure authentication succeeds.


    Args:
        df: The Pandas DataFrame to save.
        logical_date_str: The logical date used for the 'date' partition.
        base_currency: The base currency used for the 'base_currency' partition.
    """

    try:
        gcp_hook = GoogleBaseHook(gcp_conn_id="gcp_default")
        credentials = gcp_hook.get_credentials()
        storage_options = {"token": credentials}
        logger.info(
            "Successfully fetched GCP credentials from 'gcp_default' connection."
        )
    except Exception as e:
        logger.error(f"Failed to get GCP credentials from 'gcp_default': {e}")
        raise AirflowException(f"Failed to authenticate with GCP: {e}")

    partition_path = (
        f"{OUTPUT_PATH}/base_currency={base_currency}/date={logical_date_str}"
    )
    output_file = f"{partition_path}/data.parquet"

    logger.info(f"Attempting to save data to GCS path: {output_file}...")

    try:
        df.to_parquet(
            output_file,
            index=False,
            engine="pyarrow",
            compression="snappy",
            storage_options=storage_options,  # creds
        )
        logger.info(f"Data saved successfully to GCS. {len(df)} rows.")
    except Exception as e:
        logger.error(f"Failed to write Parquet to GCS: {e}")
        raise AirflowException(f"Failed to write to GCS: {e}")


@dag(
    dag_id="currency_rates_etl",
    start_date=datetime(2025, 7, 1),
    schedule_interval="@daily",
    catchup=True,
    doc_md=__doc__,
    tags=["etl", "currency", "gcs"],
    default_args={
        "owner": "airflow",
        "retries": 0,
    },
)
def currency_rates_etl_dag():
    """
    Currency Rates ETL Pipeline

    This DAG implements an idempotent pipeline to download daily
    currency exchange rates from the `frankfurter.app` API.

    **Business Logic:**
    - Fetches daily rates based on a `base_currency` (from Airflow Variables).
    - Stores data in a GCS bucket (from `output_path` Variable).
    - Data is partitioned by `base_currency` and `date` for analytics.

    **Technical Flow:**
    1.  `extract`: Fetches data from the API (retries 5 times on network failure).
    2.  `transform`: Converts JSON to a clean, ordered Pandas DataFrame.
    3.  `load`: Explicitly authenticates with `gcp_default` connection and saves the DataFrame to GCS as a Parquet file.
    """

    # '{{ ds }}' airflow macro
    logical_date = "{{ ds }}"

    json_data = extract(logical_date_str=logical_date, base_currency=BASE_CURRENCY)

    df = transform(
        data=json_data, logical_date_str=logical_date, base_currency=BASE_CURRENCY
    )

    load(df=df, logical_date_str=logical_date, base_currency=BASE_CURRENCY)


currency_rates_etl_dag()
