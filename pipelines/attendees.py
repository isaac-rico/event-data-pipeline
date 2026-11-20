"""
ATTENDEE DATA PIPELINE ORCHESTRATOR
-----------------------------------

Pulls attendee data from attendee_pull.ipynb,
loads the latest attendee snapshot into a staging table,
removes stale attendees for events included in the current pull,
then MERGEs new/updated attendees into the main attendee_data table.

Main table:
    attendee_data

Staging table:
    attendee_staging

Attendee key:
    event_id + attendee_id
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
    ATTENDEE_TABLE_ID,
    ATTENDEE_STAGING_ID
)


# ------------------------------------------------------------------
# SCHEMA
# ------------------------------------------------------------------

ATTENDEE_SCHEMA = [
    bigquery.SchemaField("phone", "STRING"),
    bigquery.SchemaField("company", "STRING"),
    bigquery.SchemaField(
        "event_id",
        "STRING",
        mode="REQUIRED"
    ),

    bigquery.SchemaField(
        "attendee_id",
        "STRING"
    ),

    bigquery.SchemaField(
        "first_name",
        "STRING"
    ),

    bigquery.SchemaField(
        "last_name",
        "STRING"
    ),

    bigquery.SchemaField(
        "email",
        "STRING"
    ),

    bigquery.SchemaField(
        "order_id",
        "STRING"
    ),

    bigquery.SchemaField(
        "order_date",
        "STRING"
    ),

    bigquery.SchemaField(
        "event_name",
        "STRING"
    ),

    bigquery.SchemaField(
        "ticket_type",
        "STRING"
    ),
]

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
            schema=ATTENDEE_SCHEMA,
        )

        client.create_table(table)

        print(f"Created {table_id}")


# ------------------------------------------------------------------
# RUN PIPELINE
# ------------------------------------------------------------------

def run_attendee_pipeline(event_df):

    records = []

    # --------------------------------------------------------
    # Pull attendees for every event we successfully retrieved
    # --------------------------------------------------------

    for _, event in event_df.iterrows():

        event_id = event["event_id"]
        event_name = event["event_name"]
        source = event["event_source"]

        print(
            f"Pulling attendees: "
            f"{event_name} ({source})"
        )

        if source == "CVENT":

            event_records = cvent.fetch_attendees(
                event_id,
                event_name,
            )

        elif source == "EVENTBRITE":

            event_records = eventbrite.get_eventbrite_attendees(
                event_id,
                event_name,
            )

        else:
            continue

        records.extend(event_records)

    attendee_df = pd.DataFrame(records)

    print(f"Attendees pulled: {len(attendee_df)}")

    if attendee_df.empty:
        raise RuntimeError(
            "Attendee pull returned zero attendees. "
            "Stopping before BigQuery is modified."
        )

    # --------------------------------------------------------
    # Validate IDs
    # --------------------------------------------------------

    invalid_ids = (
        attendee_df["attendee_id"].isna()
        |
        attendee_df["attendee_id"]
            .astype(str)
            .str.strip()
            .eq("")
    )

    if invalid_ids.any():

        bad_count = invalid_ids.sum()

        raise ValueError(
            f"{bad_count} attendees are missing attendee_id."
        )

    # attendee_df["order_date"] = pd.to_datetime(
    #     attendee_df["order_date"],
    #     errors="coerce",
    #     utc=True,
    # )

    # --------------------------------------------------------
    # BigQuery
    # --------------------------------------------------------

    client = bigquery.Client(project=PROJECT_ID)

    create_table_if_missing(
        client,
        ATTENDEE_TABLE_ID,
    )

    create_table_if_missing(
        client,
        ATTENDEE_STAGING_ID,
    )

    # Add fields to already deployed tables before loading or merging.
    for table_id in (ATTENDEE_TABLE_ID, ATTENDEE_STAGING_ID):
        client.query(f"""
            ALTER TABLE `{table_id}`
            ADD COLUMN IF NOT EXISTS phone STRING,
            ADD COLUMN IF NOT EXISTS company STRING
        """).result()

    load_config = bigquery.LoadJobConfig(
        schema=ATTENDEE_SCHEMA,
        write_disposition="WRITE_TRUNCATE",
    )

    client.load_table_from_dataframe(
        attendee_df,
        ATTENDEE_STAGING_ID,
        job_config=load_config,
    ).result()

    print("Attendee staging table loaded.")

    # --------------------------------------------------------
    # Remove stale attendees
    # --------------------------------------------------------

    delete_sql = f"""
    DELETE FROM `{ATTENDEE_TABLE_ID}` T

    WHERE T.event_id IN (
        SELECT DISTINCT event_id
        FROM `{ATTENDEE_STAGING_ID}`
    )

    AND NOT EXISTS (

        SELECT 1

        FROM `{ATTENDEE_STAGING_ID}` S

        WHERE S.event_id = T.event_id
          AND S.attendee_id = T.attendee_id
    )
    """

    client.query(delete_sql).result()

    # --------------------------------------------------------
    # MERGE
    # --------------------------------------------------------

    merge_sql = f"""
    MERGE `{ATTENDEE_TABLE_ID}` T
    USING `{ATTENDEE_STAGING_ID}` S

    ON T.event_id = S.event_id
       AND T.attendee_id = S.attendee_id

    WHEN MATCHED THEN
      UPDATE SET
        first_name = S.first_name,
        last_name = S.last_name,
        email = S.email,
        phone = S.phone,
        company = S.company,
        order_id = S.order_id,
        order_date = S.order_date,
        event_name = S.event_name,
        ticket_type = S.ticket_type

    WHEN NOT MATCHED THEN
      INSERT (
        event_id,
        attendee_id,
        first_name,
        last_name,
        email,
        phone,
        company,        order_id,
        order_date,
        event_name,
        ticket_type
      )

      VALUES (
        S.event_id,
        S.attendee_id,
        S.first_name,
        S.last_name,
        S.email,
        S.phone,
        S.company,        S.order_id,
        S.order_date,
        S.event_name,
        S.ticket_type
      )
    """

    client.query(merge_sql).result()

    print("Attendees merged into BigQuery.")