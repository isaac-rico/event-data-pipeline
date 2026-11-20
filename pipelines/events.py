"""
EVENT DATA PIPELINE ORCHESTRATOR
--------------------------------

Pulls event data from Cvent and Eventbrite,
combines the results,
loads them into a staging table,
then MERGEs them into the main events table.
"""

import pandas as pd
from google.cloud import bigquery


from pulls import cvent
from pulls import eventbrite


# ------------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------------

from config import (
    PROJECT_ID,
    EVENT_STAGING_ID,
    EVENT_TABLE_ID
)

# ------------------------------------------------------------------
# EVENT_SCHEMA
# ------------------------------------------------------------------

EVENT_SCHEMA = [
    bigquery.SchemaField("event_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("event_name", "STRING"),
    bigquery.SchemaField("event_location", "STRING"),
    bigquery.SchemaField("event_timezone", "STRING"),
    bigquery.SchemaField("event_start", "DATETIME"),
    bigquery.SchemaField("event_end", "DATETIME"),
    bigquery.SchemaField("event_capacity", "INTEGER"),
    bigquery.SchemaField("event_attendee_count", "INTEGER"),
    bigquery.SchemaField("event_source", "STRING"),
    bigquery.SchemaField("event_url", "STRING"),
]


# ------------------------------------------------------------------
# BIGQUERY CLIENT
# ------------------------------------------------------------------

client = bigquery.Client(project=PROJECT_ID)


# ------------------------------------------------------------------
# CREATE TABLES IF THEY DON'T EXIST
# ------------------------------------------------------------------

def create_table_if_missing(client, table_id):

    try:
        client.get_table(table_id)
        return

    except Exception:
        table = bigquery.Table(
            table_id,
            schema=EVENT_SCHEMA,
        )

        client.create_table(table)

        print(f"Created {table_id}")


# ------------------------------------------------------------------
# PULL EVENT DATA
# ------------------------------------------------------------------

def run_event_pipeline():

    print("Pulling Cvent events...")
    cvent_df = cvent.run_cvent_pull()

    print(f"Cvent events: {len(cvent_df)}")

    print("Pulling Eventbrite events...")
    eventbrite_df = eventbrite.fetch_events()

    print(f"Eventbrite events: {len(eventbrite_df)}")

    # --------------------------------------------------------
    # Combine
    # --------------------------------------------------------

    event_df = pd.concat(
        [cvent_df, eventbrite_df],
        ignore_index=True,
    )

    if event_df.empty:
        raise RuntimeError(
            "Event pull returned zero events. "
            "Stopping before BigQuery is modified."
        )

    event_df["event_start"] = pd.to_datetime(
        event_df["event_start"],
        errors="coerce",
    )

    event_df["event_end"] = pd.to_datetime(
        event_df["event_end"],
        errors="coerce",
    )

    print(f"Total events pulled: {len(event_df)}")

    # --------------------------------------------------------
    # BigQuery
    # --------------------------------------------------------

    client = bigquery.Client(project=PROJECT_ID)

    create_table_if_missing(client, EVENT_TABLE_ID)
    create_table_if_missing(client, EVENT_STAGING_ID)

    # Staging is always a fresh snapshot
    load_config = bigquery.LoadJobConfig(
        schema=EVENT_SCHEMA,
        write_disposition="WRITE_TRUNCATE",
    )

    client.load_table_from_dataframe(
        event_df,
        EVENT_STAGING_ID,
        job_config=load_config,
    ).result()

    print("Event staging table loaded.")

    # --------------------------------------------------------
    # MERGE
    # --------------------------------------------------------

    merge_sql = f"""
    MERGE `{EVENT_TABLE_ID}` T
    USING `{EVENT_STAGING_ID}` S

    ON T.event_id = S.event_id

    WHEN MATCHED THEN
      UPDATE SET
        event_name = S.event_name,
        event_location = S.event_location,
        event_timezone = S.event_timezone,
        event_start = S.event_start,
        event_end = S.event_end,
        event_capacity = S.event_capacity,
        event_attendee_count = S.event_attendee_count,
        event_source = S.event_source,
        event_url = S.event_url

    WHEN NOT MATCHED THEN
      INSERT (
        event_id,
        event_name,
        event_location,
        event_timezone,
        event_start,
        event_end,
        event_capacity,
        event_attendee_count,
        event_source,
        event_url
      )

      VALUES (
        S.event_id,
        S.event_name,
        S.event_location,
        S.event_timezone,
        S.event_start,
        S.event_end,
        S.event_capacity,
        S.event_attendee_count,
        S.event_source,
        S.event_url
      )
    """

    client.query(merge_sql).result()

    print("Events merged into BigQuery.")

    return event_df