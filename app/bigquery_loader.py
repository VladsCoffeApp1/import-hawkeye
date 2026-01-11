#!/usr/bin/env python3
# /// script
# requires-python = "==3.12.9"
# dependencies = []
# ///

"""
SPDX-License-Identifier: LicenseRef-NonCommercial-Only
© 2025 github.com/defmon3 — Non-commercial use only. Commercial use requires permission.
Format docstrings according to PEP 287
File: bigquery_loader.py

Unified BigQuery loading for all data types.
"""

import pandas as pd
from google.cloud import bigquery
from loguru import logger as log

from .config import settings
from .custom_exceptions import LoadError
from .detector import DataType
from .schemas import get_schema


def get_or_create_table(
    client: bigquery.Client,
    project: str,
    dataset: str,
    table: str,
    data_type: DataType,
) -> bigquery.Table:
    """
    Get existing table or create new one with schema.

    :param client: BigQuery client
    :param project: GCP project ID
    :param dataset: BigQuery dataset name
    :param table: BigQuery table name
    :param data_type: Data type for schema lookup
    :returns: BigQuery Table object
    """
    table_id = f"{project}.{dataset}.{table}"
    schema_config = get_schema(data_type)

    try:
        table_ref = client.get_table(table_id)
        log.debug(f"Table {table_id} exists")
        return table_ref
    except Exception:
        log.info(f"Creating table {table_id}")

        table_ref = bigquery.Table(table_id, schema=schema_config.bigquery_schema)

        # Configure partitioning
        table_ref.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=schema_config.partition_field,
        )

        # Configure clustering
        table_ref.clustering_fields = schema_config.cluster_fields

        table_ref = client.create_table(table_ref)
        log.info(f"Created table {table_id}")
        return table_ref


def filter_schema_to_dataframe(
    schema: list[bigquery.SchemaField],
    df: pd.DataFrame,
) -> list[bigquery.SchemaField]:
    """
    Filter schema to only include fields present in the dataframe.

    :param schema: Full BigQuery schema
    :param df: DataFrame with actual columns
    :returns: Filtered schema matching dataframe columns
    """
    df_columns = set(df.columns)
    filtered = [field for field in schema if field.name in df_columns]
    log.debug(f"Filtered schema from {len(schema)} to {len(filtered)} fields")
    return filtered


def load_to_bigquery(
    df: pd.DataFrame,
    data_type: DataType,
    project: str | None = None,
    dataset: str | None = None,
    table: str | None = None,
) -> int:
    """
    Load DataFrame to BigQuery with deduplication.

    Uses MERGE to insert only rows where document_id doesn't already exist.

    :param df: DataFrame to load
    :param data_type: Data type for schema lookup
    :param project: GCP project ID (defaults to settings)
    :param dataset: BigQuery dataset name (defaults to settings)
    :param table: BigQuery table name (required)
    :returns: Number of rows loaded
    :raises LoadError: If loading fails
    """
    project = project or settings.project_id
    dataset = dataset or settings.bq_dataset

    if not table:
        raise LoadError("Table name is required")

    table_id = f"{project}.{dataset}.{table}"
    log.info(f"Loading {len(df)} rows to {table_id} (with deduplication)")

    try:
        client = bigquery.Client(project=project)

        # Ensure target table exists
        get_or_create_table(client, project, dataset, table, data_type)

        # Get schema config including dedup key
        schema_config = get_schema(data_type)
        filtered_schema = filter_schema_to_dataframe(schema_config.bigquery_schema, df)
        dedup_key = schema_config.dedup_key

        # Load to temp staging table
        staging_table = f"{table}_staging"
        staging_table_id = f"{project}.{dataset}.{staging_table}"

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=filtered_schema,
        )

        log.debug(f"Loading to staging table {staging_table_id}")
        job = client.load_table_from_dataframe(df, staging_table_id, job_config=job_config)
        job.result()

        # Build column list for MERGE
        columns = [field.name for field in filtered_schema]
        column_list = ", ".join(columns)
        source_columns = ", ".join([f"s.{col}" for col in columns])

        # Build MERGE ON clause from schema dedup_key
        # Non-string columns (FLOAT, TIMESTAMP, INT, DATE) cannot use COALESCE with ''
        non_string_columns = set(
            schema_config.float_columns
            + schema_config.timestamp_columns
            + schema_config.int_columns
            + [schema_config.partition_field]  # DATE field
        )
        log.debug(f"Using dedup key: {dedup_key}")
        on_conditions = [
            f"t.{col} = s.{col}" if col in non_string_columns
            else f"COALESCE(t.{col}, '') = COALESCE(s.{col}, '')"
            for col in dedup_key
        ]
        on_clause = " AND ".join(on_conditions)

        merge_query = f"""
        MERGE `{table_id}` t
        USING `{staging_table_id}` s
        ON {on_clause}
        WHEN NOT MATCHED THEN
            INSERT ({column_list})
            VALUES ({source_columns})
        """

        log.debug("Executing MERGE for deduplication")
        merge_job = client.query(merge_query)
        merge_job.result()

        rows_inserted = merge_job.num_dml_affected_rows or 0
        rows_skipped = len(df) - rows_inserted

        if rows_skipped > 0:
            log.info(f"Skipped {rows_skipped} duplicate rows")

        # Clean up staging table
        client.delete_table(staging_table_id, not_found_ok=True)

        log.info(f"Loaded {rows_inserted} new rows to {table_id}")
        return rows_inserted

    except Exception as e:
        log.error(f"Failed to load to {table_id}: {e}")
        raise LoadError(f"Failed to load data to BigQuery: {e}") from e
